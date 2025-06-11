"""
Script to process ZASCA data and create a unified CSV file.

This script reads data from multiple ZASCA cohort files, processes them,
and saves a unified CSV file for later use.
"""

import logging
from pathlib import Path

from innpulsa.processing import read_and_process_zasca
from innpulsa.processing.rues import read_rues
from innpulsa.settings import DATA_DIR

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def main():
    """Main function to process ZASCA data."""
    try:
        logger.info("starting ZASCA data processing")

        # (re)process ZASCA data
        zasca_df = read_and_process_zasca()

        # load RUES data (processed total CSV)
        rues_df = read_rues()

        # remove "-\d" from zasca NITs [VALIDATE]
        zasca_df["nit"] = zasca_df["nit"].astype(str).str.replace(r"-\d+", "")

        # create boolean column indicating if ZASCA NIT appears in RUES
        rues_nits = set(rues_df["numero_de_identificacion"].astype(str))
        zasca_df["in_rues"] = zasca_df["nit"].astype(str).isin(rues_nits)

        # save enhanced ZASCA dataset
        output_dir = Path(DATA_DIR) / "processed"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "zasca_total.csv"
        logger.info(
            "saving enhanced ZASCA data with RUES match column -> %s", output_path
        )
        zasca_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        logger.info(
            "successfully processed %d ZASCA records (RUES matches added) and saved to CSV",
            len(zasca_df),
        )

    except Exception as e:
        logger.error("failed to process ZASCA data: %s", str(e))
        raise


if __name__ == "__main__":
    main()
