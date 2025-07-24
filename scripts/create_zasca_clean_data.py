"""
Script to process ZASCA data and create a unified CSV file.

This script reads data from multiple ZASCA cohort files, processes them,
and saves a unified CSV file for later use.
"""

from pathlib import Path

from innpulsa.loaders import load_zasca, load_rues
from innpulsa.logging import configure_logger
from innpulsa.processing import process_zasca
from innpulsa.settings import DATA_DIR

logger = configure_logger("innpulsa.scripts.create_zasca_clean_data")


def main():
    """
    Run function to process ZASCA data.

    This script reads data from multiple ZASCA cohort files, processes them,
    and saves a unified CSV file for later use.

    """
    logger.info("starting ZASCA data processing")

    # (re)process ZASCA data
    zasca_df = load_zasca()

    # load RUES data (processed total CSV)
    logger.info("loading RUES data")
    rues_df = load_rues()

    logger.info("Processing ZASCA data")
    zasca_df = process_zasca(zasca_df)

    # create boolean column indicating if ZASCA NIT appears in RUES
    logger.info("creating RUES match column")
    rues_nits = list(set(rues_df["numero_de_identificacion"].astype(str)))
    zasca_df["in_rues"] = zasca_df["nit"].astype(str).isin(rues_nits)

    # save enhanced ZASCA dataset
    output_dir = Path(DATA_DIR) / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "zasca_total.csv"

    logger.info("saving processed ZASCA data to %s", output_path)
    zasca_df.to_csv(output_path, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
