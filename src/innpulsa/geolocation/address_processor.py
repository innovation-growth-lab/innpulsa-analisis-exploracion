"""Shared address processing functionality for RUES and ZASCA datasets."""

import json
import os
from pathlib import Path

import pandas as pd

from innpulsa.geolocation.llm import normalise_addresses_using_llm
from innpulsa.logging import configure_logger
from innpulsa.settings import DATA_DIR

logger = configure_logger("innpulsa.geolocation.address_processor")


class AddressProcessor:
    """Handles address processing for both RUES and ZASCA datasets."""

    def __init__(self, dataset: str, subdirectory: str | None = None):
        """Initialise processor for a specific dataset.

        Args:
            dataset: Either 'rues' or 'zasca'
            subdirectory: Optional subdirectory name for dataset variants (e.g., 'five_centers', 'closed')

        Raises:
            ValueError: If dataset is not 'rues' or 'zasca'.

        """
        if dataset not in {"rues", "zasca"}:
            error_msg = f"invalid dataset: {dataset}"
            raise ValueError(error_msg)

        self.dataset = dataset
        self.subdirectory = subdirectory

        # Create output directory path with optional subdirectory
        if subdirectory:
            self.output_dir = Path(DATA_DIR) / f"02_processed/geolocation/{subdirectory}/{dataset}_addresses"
        else:
            self.output_dir = Path(DATA_DIR) / f"02_processed/geolocation/{dataset}_addresses"

        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(
            "initialised processor for %s with output directory: %s",
            dataset,
            self.output_dir,
        )

    @staticmethod
    def _normalise_city(series: pd.Series) -> pd.Series:
        """Lower-case, strip, and remove accents from city names.

        Args:
            series: Series of city names to normalise

        Returns:
            Series of normalised city names

        """
        return (
            series.fillna("")
            .str.lower()
            .str.strip()
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )

    @staticmethod
    def _enrich_zasca_with_ciiu(zasca: pd.DataFrame, rues: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich ZASCA data with CIIU codes by merging with RUES on NIT.

        Args:
            zasca: DataFrame containing ZASCA data
            rues: DataFrame containing RUES data

        Returns:
            DataFrame with CIIU codes added from RUES

        """
        # Ensure consistent data types for merging
        zasca = zasca.copy()
        if "nit" in zasca.columns:
            zasca["nit"] = zasca["nit"].astype(str).str.strip()
        rues_nit_ciiu = rues[["nit", "ciiu_principal"]].copy()
        rues_nit_ciiu["nit"] = rues_nit_ciiu["nit"].astype(str).str.strip()
        rues_nit_ciiu["ciiu_principal"] = rues_nit_ciiu["ciiu_principal"].astype(str).str.strip()

        return zasca.merge(rues_nit_ciiu, on="nit", how="left")

    @staticmethod
    def _get_top_ciius_per_city(zasca_with_ciiu: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
        """
        Identify top N CIIU codes per city in ZASCA data.

        Args:
            zasca_with_ciiu: DataFrame containing ZASCA data with CIIU codes
            top_n: Number of top CIIU codes to identify per city

        Returns:
            DataFrame containing the top N CIIU codes per city

        """
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
        return pd.DataFrame(city_ciiu_counts[["city_norm", "ciiu_principal"]])

    @staticmethod
    def _filter_rues_by_city_ciius(rues: pd.DataFrame, city_ciius: pd.DataFrame) -> pd.DataFrame:
        """
        Filter RUES to only include records matching city-specific CIIU codes.

        Args:
            rues: DataFrame containing RUES data
            city_ciius: DataFrame containing city-specific CIIU codes

        Returns:
            DataFrame containing filtered RUES data

        """
        # Create a set of valid (city, ciiu) combinations for fast lookup
        valid_combinations = set(
            zip(
                city_ciius["city_norm"],
                city_ciius["ciiu_principal"],
                strict=False,
            )
        )

        # Filter RUES records that match valid city-CIIU combinations
        rues_filtered = rues[
            rues.apply(
                lambda row: (row["city_norm"], row["ciiu_principal"]) in valid_combinations,
                axis=1,
            )
        ].copy()

        logger.info(
            "Filtered RUES from %d to %d records using city-specific CIIU mat",
            len(rues),
            len(rues_filtered),
        )
        return pd.DataFrame(rues_filtered)

    @staticmethod
    def _sample_with_city_weights(filtered_rues: pd.DataFrame, zasca: pd.DataFrame, target_n: int) -> pd.DataFrame:
        """
        Sample RUES records with city-based weighting.

        Args:
            filtered_rues: DataFrame containing filtered RUES data
            zasca: DataFrame containing ZASCA data
            target_n: Target number of records to sample

        Returns:
            DataFrame containing sampled RUES data

        """
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

        filtered_rues["weight"] = filtered_rues["city_norm"].map(city_weights).fillna(1e-6)

        sampled_rues = filtered_rues.sample(
            n=target_n,
            weights=filtered_rues["weight"],
            random_state=42,
            replace=False,
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
        """
        Filter RUES data to include only rows matching ZASCA cities and their
        top CIIU codes.

        Args:
            rues: DataFrame containing RUES records with city, state, and
            ciiu_principal columns
            zasca: DataFrame containing ZASCA records with city and nit columns
            target_n: Target number of rows to sample

        Returns:
            DataFrame containing filtered RUES records matching ZASCA
            city-specific CIIU patterns.

        """
        # Normalize city names for consistent matching
        zasca.loc[zasca["city"] == "Donmatías", "city"] = "Don Matías"
        zasca["city_norm"] = self._normalise_city(pd.Series(zasca["city"]))
        rues["city_norm"] = self._normalise_city(pd.Series(rues["city"]))
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

    @staticmethod
    def build_rues_address(df: pd.DataFrame) -> pd.DataFrame:
        """
        Create full_address and id columns expected by LLM for RUES data.

        Args:
            df: DataFrame containing RUES data

        Returns:
            DataFrame with full_address and id columns

        """
        # Create full_address column
        df = df.copy()
        df["full_address"] = (
            df["dirección_comercial"].fillna("") + ", " + df["city"].fillna("") + ", " + df["state"].fillna("") + ", CO"
        )

        # LLM helper expects identifier column named numberid_emp1
        df["numberid_emp1"] = df["nit"]
        return df

    @staticmethod
    def build_zasca_address(df: pd.DataFrame) -> pd.DataFrame:
        """
        Create full_address column for ZASCA data (placeholder).

        Args:
            df: DataFrame containing ZASCA data

        Returns:
            DataFrame with full_address column

        """
        df = df.copy()
        # check full_address exists
        if "full_address" not in df.columns:
            logger.warning("ZASCA data missing full_address column")
        return df

    async def process_addresses(
        self,
        df: pd.DataFrame,
        prompt: str,
        filter_against_zasca: pd.DataFrame | None = None,
        target_n: int = 520,
    ) -> pd.DataFrame | None:
        """Process addresses from dataframe and compile results.

        Args:
            df: DataFrame containing address data
            prompt: LLM prompt to use for processing
            filter_against_zasca: Optional ZASCA DataFrame for filtering RUES
                data by city and ciiu_principal
            target_n: Target number of rows for RUES filtering

        Returns:
            Processed DataFrame with standardized addresses

        Raises:
            ValueError: If missing full_address column in input data

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
            error_msg = "missing full_address column in input data"
            logger.error(error_msg)
            raise ValueError(error_msg)

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

    def _compile_results(self) -> pd.DataFrame | None:
        """
        Compile all results into a DataFrame.

        Returns:
            DataFrame containing compiled results

        """
        records = []
        success_files = list(self.output_dir.glob("batch_*_success.json"))
        logger.debug("found %d successful batch files", len(success_files))

        for batch_file in success_files:
            try:
                with Path.open(batch_file, encoding="utf-8") as f:
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
                            "skip invalid result for ID %s in %s: not a dict",
                            id_,
                            batch_file,
                        )
                        continue

                    # Use appropriate ID column name based on dataset
                    id_column = "nit" if self.dataset == "rues" else "id"

                    records.append({
                        id_column: id_,
                        "raw_address": batch["input_addresses"].get(id_, ""),
                        "formatted_address": result.get("formatted_address"),
                        "country": result.get("country"),
                        "area": result.get("area"),
                        "city": result.get("city"),
                    })
            except json.JSONDecodeError as e:
                error_msg = f"JSON parsing error in batch file {batch_file}: {e}"
                logger.exception(error_msg)
                continue
            except KeyError as e:
                error_msg = f"missing required field {e} in batch file {batch_file}"
                logger.exception(error_msg)
                continue
            except Exception as e:  # pylint: disable=W0718
                error_msg = f"unexpected error processing batch file {batch_file}: {e}"
                logger.exception(error_msg)
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
        # Create output path with optional subdirectory
        if self.subdirectory:
            output_file = Path(DATA_DIR) / f"02_processed/geolocation/{self.subdirectory}/{self.dataset}_addresses.csv"
        else:
            output_file = Path(DATA_DIR) / f"02_processed/geolocation/{self.dataset}_addresses.csv"

        output_file.parent.mkdir(parents=True, exist_ok=True)
        results_df.to_csv(output_file, index=False, encoding="utf-8-sig")
        logger.info("saved %d records to %s", len(results_df), output_file)
        return output_file
