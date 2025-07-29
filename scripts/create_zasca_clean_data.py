"""
Script to process ZASCA data and create a unified CSV file.

This script reads data from multiple ZASCA cohort files, processes them,
and saves a unified CSV file for later use.

Usage:
    python scripts/create_zasca_clean_data.py --mode five_centers
    python scripts/create_zasca_clean_data.py --mode closed
"""

import argparse
from pathlib import Path

from innpulsa.loaders import load_five_centers_zasca, load_closed_zascas, load_rues
from innpulsa.logging import configure_logger
from innpulsa.processing import process_zasca
from innpulsa.settings import DATA_DIR

logger = configure_logger("innpulsa.scripts.create_zasca_clean_data")


def main(mode: str):
    """
    Run function to process ZASCA data.

    This script reads data from multiple ZASCA cohort files, processes them,
    and saves a unified CSV file for later use.

    Args:
        mode: Either 'five_centers' or 'closed' to determine which loader to use
    """
    logger.info("starting ZASCA data processing with mode: %s", mode)

    # Load ZASCA data based on mode
    if mode == "five_centers":
        logger.info("loading five centers ZASCA data")
        zasca_df = load_five_centers_zasca()
        output_filename = "zasca_five_centers.csv"
    else:  # mode == "closed"
        logger.info("loading closed ZASCAs data")
        zasca_df = load_closed_zascas()
        output_filename = "zasca_closed.csv"

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
    output_path = output_dir / output_filename

    logger.info("saving processed ZASCA data to %s", output_path)
    zasca_df.to_csv(output_path, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process ZASCA data and create a unified CSV file.")
    parser.add_argument(
        "--mode",
        choices=["five_centers", "closed"],
        default="five_centers",
        help="Which ZASCA data to process: five_centers for the 5 center cohorts, closed for all closed ZASCAs",
    )

    args = parser.parse_args()
    main(args.mode)
