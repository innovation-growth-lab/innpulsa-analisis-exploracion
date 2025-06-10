"""
Script to process ZASCA data and create a unified CSV file.

This script reads data from multiple ZASCA cohort files, processes them,
and saves a unified CSV file for later use.
"""

import logging
from innpulsa.processing import read_and_process_zasca

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def main():
    """Main function to process ZASCA data."""
    try:
        logger.info("starting ZASCA data processing")

        # read and process ZASCA data (will save to data/processed/zasca_total.csv)
        zasca_df = read_and_process_zasca(save_processed=True)

        logger.info(
            "successfully processed %d ZASCA records and saved to CSV", len(zasca_df)
        )

    except Exception as e:
        logger.error("failed to process ZASCA data: %s", str(e))
        raise


if __name__ == "__main__":
    main()
