"""Shared address processing functionality for RUES and ZASCA datasets."""

import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from innpulsa.geolocation.llm import normalise_addresses_using_llm
from innpulsa.settings import DATA_DIR
from innpulsa.logging import configure_logger

logger = configure_logger("innpulsa.geolocation.address_processor")


class AddressProcessor:
    """Handles address processing for both RUES and ZASCA datasets."""

    def __init__(self, dataset: str):
        """Initialise processor for a specific dataset.

        Args:
            dataset: Either 'rues' or 'zasca'
        """
        if dataset not in ["rues", "zasca"]:
            raise ValueError("dataset must be 'rues' or 'zasca'")

        self.dataset = dataset
        self.output_dir = Path(DATA_DIR) / f"processed/geolocation/{dataset}_addresses"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(
            "initialised processor for %s with output directory: %s",
            dataset,
            self.output_dir,
        )

    def _normalise_city(self, series: pd.Series) -> pd.Series:
        """Lower-case, strip, and remove accents from city names."""
        return (
            series.fillna("")
            .str.lower()
            .str.strip()
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )

    def _enrich_zasca_with_ciiu(
        self, zasca: pd.DataFrame, rues: pd.DataFrame
    ) -> pd.DataFrame:
        """Enrich ZASCA data with CIIU codes by merging with RUES on NIT."""
        # Ensure consistent data types for merging
        zasca = zasca.copy()
        if "nit" in zasca.columns:
            zasca["nit"] = zasca["nit"].astype(str).str.strip()
        rues_nit_ciiu = rues[["nit", "ciiu_principal"]].copy()
        rues_nit_ciiu["nit"] = rues_nit_ciiu["nit"].astype(str).str.strip()
        rues_nit_ciiu["ciiu_principal"] = (
            rues_nit_ciiu["ciiu_principal"].astype(str).str.strip()
        )

        return zasca.merge(rues_nit_ciiu, on="nit", how="left")

    def _get_top_ciius_per_city(
        self, zasca_with_ciiu: pd.DataFrame, top_n: int = 5
    ) -> pd.DataFrame:
        """Identify top N CIIU codes per city in ZASCA data."""
        city_ciiu_counts = (
            zasca_with_ciiu.dropna(subset=["city_norm", "ciiu_principal"])
            .groupby(["city_norm", "ciiu_principal"])
            .size()
            .reset_index(name="count")
            .sort_values(["city_norm", "count"], ascending=[True, False])
            .groupby("city_norm")
            .head(top_n)
        )

        logger.info(
            "Identified top %d CIIUs for %d cities from ZASCA data",
            top_n,
            city_ciiu_counts["city_norm"].nunique(),
        )
        return city_ciiu_counts[["city_norm", "ciiu_principal"]]

    def _filter_rues_by_city_ciius(
        self, rues: pd.DataFrame, city_ciius: pd.DataFrame
    ) -> pd.DataFrame:
        """Filter RUES data to only include records matching city-specific CIIU codes."""
        # Create a set of valid (city, ciiu) combinations for fast lookup
        valid_combinations = set(
            zip(city_ciius["city_norm"], city_ciius["ciiu_principal"])
        )

        # Filter RUES records that match valid city-CIIU combinations
        rues_filtered = rues[
            rues.apply(
                lambda row: (row["city_norm"], row["ciiu_principal"])
                in valid_combinations,
                axis=1,
            )
        ].copy()

        logger.info(
            "Filtered RUES from %d to %d records using city-specific CIIU matching",
            len(rues),
            len(rues_filtered),
        )
        return rues_filtered

    def _sample_with_city_weights(
        self, filtered_rues: pd.DataFrame, zasca: pd.DataFrame, target_n: int
    ) -> pd.DataFrame:
        """Sample RUES records with city-based weighting."""
        if len(filtered_rues) <= target_n:
            logger.info(
                "Taking all %d available records (≤ target %d)",
                len(filtered_rues),
                target_n,
            )
            return filtered_rues

        # Calculate city-based sampling weights
        zasca_city_counts = zasca.groupby("city_norm").size()
        rues_city_counts = filtered_rues.groupby("city_norm").size()
        city_weights = (zasca_city_counts / rues_city_counts).fillna(1e-6)

        filtered_rues["weight"] = (
            filtered_rues["city_norm"].map(city_weights).fillna(1e-6)
        )

        sampled_rues = filtered_rues.sample(
            n=target_n, weights=filtered_rues["weight"], random_state=42, replace=False
        )

        logger.info(
            "Sampled %d RUES records from %d available",
            len(sampled_rues),
            len(filtered_rues),
        )
        return sampled_rues.drop(columns=["weight"])

    def filter_rues_against_zasca(
        self,
        rues: pd.DataFrame,
        zasca: pd.DataFrame,
        target_n: int = 520,
    ) -> pd.DataFrame:
        """Filter RUES data to include only rows matching ZASCA cities and their top CIIU codes.

        Args:
            rues: DataFrame containing RUES records with city, state, and ciiu_principal columns
            zasca: DataFrame containing ZASCA records with city and nit columns
            target_n: Target number of rows to sample

        Returns:
            DataFrame containing filtered RUES records matching ZASCA city-specific CIIU patterns
        """
        rues = rues.copy()
        zasca = zasca.copy()

        # Normalize city names for consistent matching
        zasca.loc[zasca["city"] == "Donmatías", "city"] = "Don Matías"
        zasca["city_norm"] = self._normalise_city(zasca["city"])
        rues["city_norm"] = self._normalise_city(rues["city"])
        rues["ciiu_principal"] = rues["ciiu_principal"].astype(str).str.strip()

        # Step 1: Enrich ZASCA with CIIU data from RUES
        zasca_with_ciiu = self._enrich_zasca_with_ciiu(zasca, rues)

        # Step 2: Identify top CIIUs per city in ZASCA
        city_ciius = self._get_top_ciius_per_city(zasca_with_ciiu, top_n=5)

        # Step 3: Filter RUES by city-specific CIIU patterns
        filtered_rues = self._filter_rues_by_city_ciius(rues, city_ciius)

        # Step 4: Sample with city-based weighting
        sampled_rues = self._sample_with_city_weights(filtered_rues, zasca, target_n)

        return sampled_rues.drop(columns=["city_norm"], errors="ignore")

    def build_rues_address(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create full_address and id columns expected by LLM helpers for RUES data."""
        df = df.copy()
        df["full_address"] = (
            df["dirección_comercial"].fillna("")
            + ", "
            + df["city"].fillna("")
            + ", "
            + df["state"].fillna("")
            + ", CO"
        )

        # LLM helper expects identifier column named numberid_emp1 – create alias
        df["numberid_emp1"] = df["nit"]
        return df

    def build_zasca_address(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create full_address column for ZASCA data."""
        df = df.copy()
        # ZASCA data already has the expected structure, just ensure full_address exists
        if "full_address" not in df.columns:
            logger.warning("ZASCA data missing full_address column")
        return df

    async def process_addresses(
        self,
        df: pd.DataFrame,
        prompt: str,
        filter_against_zasca: Optional[pd.DataFrame] = None,
        target_n: int = 520,
    ) -> Optional[pd.DataFrame]:
        """Process addresses from dataframe and compile results.

        Args:
            df: DataFrame containing address data
            prompt: LLM prompt to use for processing
            filter_against_zasca: Optional ZASCA DataFrame for filtering RUES data by city and ciiu_principal
            target_n: Target number of rows for RUES filtering

        Returns:
            Processed DataFrame with standardized addresses
        """
        if not os.getenv("GEMINI_API_KEY"):
            logger.error("GEMINI_API_KEY environment variable is required")
            return None

        # Apply dataset-specific preprocessing
        if self.dataset == "rues":
            if filter_against_zasca is not None:
                df = self.filter_rues_against_zasca(df, filter_against_zasca, target_n)
            df = self.build_rues_address(df)
        elif self.dataset == "zasca":
            df = self.build_zasca_address(df)

        if "full_address" not in df.columns:
            logger.error("invalid data: missing full_address column")
            raise ValueError("missing full_address column in input data")

        # Process addresses using LLM
        logger.info("starting %s address processing", self.dataset)
        await normalise_addresses_using_llm(df, self.output_dir, prompt)

        # Compile results
        logger.info("compiling results")
        results_df = self._compile_results()
        if results_df is None:
            logger.warning("no results to compile")
            return None

        return results_df

    def _compile_results(self) -> Optional[pd.DataFrame]:
        """Compile all results into a DataFrame."""
        records = []
        success_files = list(self.output_dir.glob("batch_*_success.json"))
        logger.debug("found %d successful batch files", len(success_files))

        for batch_file in success_files:
            try:
                with open(batch_file, "r", encoding="utf-8") as f:
                    batch = json.load(f)

                response = json.loads(batch["response"])

                if not isinstance(response, dict):
                    logger.error(
                        "invalid response format in %s: expected dict, got %s",
                        batch_file,
                        type(response),
                    )
                    continue

                for id_, result in response.items():
                    if not isinstance(result, dict):
                        logger.warning(
                            "skipping invalid result for ID %s in %s: not a dict",
                            id_,
                            batch_file,
                        )
                        continue

                    # Use appropriate ID column name based on dataset
                    id_column = "nit" if self.dataset == "rues" else "id"

                    records.append(
                        {
                            id_column: id_,
                            "raw_address": batch["input_addresses"].get(id_, ""),
                            "formatted_address": result.get("formatted_address"),
                            "country": result.get("country"),
                            "area": result.get("area"),
                            "city": result.get("city"),
                        }
                    )
            except json.JSONDecodeError as e:
                logger.error(
                    "JSON parsing error in batch file %s: %s", batch_file, str(e)
                )
                continue
            except KeyError as e:
                logger.error(
                    "missing required field %s in batch file %s", str(e), batch_file
                )
                continue
            except Exception as e:  # pylint: disable=W0718
                logger.error(
                    "unexpected error processing batch file %s: %s",
                    batch_file,
                    str(e),
                )
                continue

        if not records:
            logger.warning("no valid records found in batch files")
            return None

        logger.debug("successfully compiled %d records", len(records))
        return pd.DataFrame(records)

    def save_results(self, results_df: pd.DataFrame) -> Path:
        """Save processed results to CSV file.

        Args:
            results_df: DataFrame with processed addresses

        Returns:
            Path to the saved file
        """
        output_file = (
            Path(DATA_DIR) / f"processed/geolocation/{self.dataset}_addresses.csv"
        )
        output_file.parent.mkdir(parents=True, exist_ok=True)
        results_df.to_csv(output_file, index=False, encoding="utf-8-sig")
        logger.info("saved %d records to %s", len(results_df), output_file)
        return output_file
