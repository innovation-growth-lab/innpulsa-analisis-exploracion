"""geolocation processing functionality."""

import json
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from innpulsa.geolocation.llm import normalise_addresses_using_llm
from innpulsa.settings import DATA_DIR
from innpulsa.logging import configure_logger


logger = configure_logger("innpulsa.geolocation.processing")


class GeolocationProcessor:
    """handles ZASCA address geolocation processing."""

    def __init__(self):
        self.output_dir = Path(DATA_DIR) / "processed/geolocation/zasca_addresses"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.debug("initialised processor with output directory: %s", self.output_dir)

    async def process_and_compile(self, df: pd.DataFrame) -> Tuple[Optional[Path], int]:
        """process addresses from dataframe and compile results.

        Args:
            df: dataframe containing addresses in 'full_address' column
        """
        if "full_address" not in df.columns:
            logger.error("invalid data: missing full_address column")
            raise ValueError("missing full_address column in input data")

        # process addresses
        logger.info("starting address processing")
        await normalise_addresses_using_llm(df, self.output_dir)

        # compile results
        logger.info("compiling results")
        results_df = self._compile_results()
        if results_df is None:
            logger.warning("no results to compile")
            return None, 0

        # save to CSV
        output_file = Path(DATA_DIR) / "processed/geolocation/zasca_addresses.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        results_df.to_csv(output_file, index=False, encoding="latin1")
        logger.info("saved %d records to %s", len(results_df), output_file)
        return output_file, len(results_df)

    def _compile_results(self) -> Optional[pd.DataFrame]:
        """compile all results into a DataFrame."""
        records = []
        success_files = list(self.output_dir.glob("batch_*_success.json"))
        logger.debug("found %d successful batch files", len(success_files))

        for batch_file in success_files:
            try:
                with open(batch_file, "r", encoding="utf-8") as f:
                    batch = json.load(f)

                # The response should already be clean JSON at this point
                response = json.loads(batch["response"])

                # Validate response structure
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

                    # Extract fields with proper null handling
                    records.append(
                        {
                            "id": id_,
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

    def save_results(self, output_file: Optional[Path] = None) -> Optional[Path]:
        """Compile and save results to CSV."""
        df = self._compile_results()
        if df is None:
            return None

        output_file = output_file or self.output_dir.parent / "zasca_addresses.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False)
        return output_file
