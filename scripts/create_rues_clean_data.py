"""Script to process RUES data.

Usage:
    python scripts/create_rues_clean_data.py
"""

from pathlib import Path
from innpulsa.settings import DATA_DIR
from innpulsa.processing.rues import process_rues
from innpulsa.loaders import load_rues, load_zipcodes_co
from innpulsa.logging import configure_logger
import pandas as pd

logger = configure_logger("innpulsa.scripts.create_rues_clean_data")


def main():
    """
    Create a clean RUES dataset.

    This script will:
    - Load the RUES data
    - Load the zipcode lookup
    - Process the RUES data
    - Save the processed RUES data

    """
    logger.info("reading and combining RUES datasets")
    rues_df = load_rues()

    logger.info("load zipcode lookup")
    zip_df = load_zipcodes_co(as_dataframe=True)
    logger.info("loaded %d zipcodes", len(zip_df))

    if not isinstance(zip_df, pd.DataFrame):
        zip_df = pd.DataFrame(zip_df)
    rues_df = process_rues(rues_df, zip_df)
    logger.info("Dataset shape: %s", rues_df.shape)

    output_dir = Path(DATA_DIR) / "02_processed"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "rues_total.csv"

    logger.info("saving processed RUES data to %s", output_path)
    rues_df.to_csv(output_path, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
