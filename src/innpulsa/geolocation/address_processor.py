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

    def filter_rues_against_zasca(
        self,
        rues: pd.DataFrame,
        zasca: pd.DataFrame,
        target_n: int = 520,
    ) -> pd.DataFrame:
        """Filter RUES data to only include rows whose city appears in ZASCA.

        Args:
            rues: DataFrame containing RUES records with city and state columns
            zasca: DataFrame containing ZASCA records with city and state columns
            target_n: Target number of rows to sample

        Returns:
            DataFrame containing filtered RUES records that have cities present in ZASCA
        """
        # [AD-HOC] ZASCA fixes
        zasca = zasca.copy()
        rues = rues.copy()
        zasca.loc[zasca["city"] == "Donmatías", "city"] = "Don Matías"
        zasca["city_norm"] = self._normalise_city(zasca["city"])
        rues["city_norm"] = self._normalise_city(rues["city"])

        shared_cities = zasca["city_norm"].dropna().unique()
        filtered = rues[rues["city_norm"].isin(shared_cities)].copy()

        total_available = len(filtered)
        if total_available <= target_n:
            logger.info(
                "Filtered RUES rows (%d) ≤ target %d – taking all of them.",
                total_available,
                target_n,
            )
            return filtered.drop(columns=["city_norm"])

        # build sampling weight per row = ZASCA_city_freq / RUES_city_freq
        z_freq = zasca.groupby("city_norm").size().rename("z_freq")
        r_freq = filtered.groupby("city_norm").size().rename("r_freq")
        freq_df = pd.concat([z_freq, r_freq], axis=1).reset_index()

        # weight per city proportional to how under-represented it is in RUES relative to ZASCA
        freq_df["weight"] = freq_df["z_freq"] / freq_df["r_freq"]

        filtered = filtered.merge(
            freq_df[["city_norm", "weight"]], on="city_norm", how="left"
        )

        # Normalise weights to avoid NaNs or zeros
        filtered["weight"].fillna(1e-6, inplace=True)

        sample_df = filtered.sample(
            n=target_n,
            replace=False,
            weights=filtered["weight"],
            random_state=42,
        )

        logger.info(
            "Filtered RUES from %d ➜ %d rows (sampled to target %d)",
            len(rues),
            len(sample_df),
            target_n,
        )

        return sample_df.drop(columns=["city_norm", "weight"])

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
            filter_against_zasca: Optional ZASCA DataFrame for filtering RUES data
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
