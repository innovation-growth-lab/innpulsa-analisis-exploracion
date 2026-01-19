"""
Script to process ZASCA data and create a unified CSV file.

This script reads data from multiple ZASCA cohort files, processes them,
and saves a unified CSV file for later use.

Usage:
    python scripts/create_zasca_clean_data.py
"""

import argparse
from pathlib import Path

from innpulsa.loaders import load_zascas, load_rues
from innpulsa.logging import configure_logger
from innpulsa.processing import process_zasca
from innpulsa.settings import DATA_DIR

logger = configure_logger("innpulsa.scripts.create_zasca_clean_data")


def main(sector: str = "manufactura") -> None:
    """
    Run function to process ZASCA data.

    This script reads data from multiple ZASCA cohort files, processes them,
    and saves a unified CSV file for later use.

    Args:
        sector: The sector to filter by ("manufactura" or "agro")

    Raises:
        ValueError: If sector is not "manufactura" or "agro"

    """
    logger.info("starting ZASCA data processing for %s sector", sector)

    logger.info("loading ZASCA data")
    zasca_df = load_zascas()

    # Filter by GRUPOS12 based on sector
    manufacturing_group = 3
    agriculture_group = 1

    if sector == "manufactura":
        zasca_df = zasca_df.loc[zasca_df["GRUPOS12"] == manufacturing_group].copy()
        logger.info("filtered to manufacturing sector (GRUPOS12=%d): %d records", manufacturing_group, len(zasca_df))
    elif sector == "agro":
        zasca_df = zasca_df.loc[zasca_df["GRUPOS12"] == agriculture_group].copy()
        logger.info("filtered to agriculture sector (GRUPOS12=%d): %d records", agriculture_group, len(zasca_df))
    else:
        error_msg = "sector must be 'manufactura' or 'agro'"
        raise ValueError(error_msg)

    # load RUES data (processed total CSV)
    logger.info("loading RUES data")
    rues_df = load_rues()

    logger.info("Processing ZASCA data")
    zasca_df = process_zasca(zasca_df)

    # create boolean column indicating if ZASCA NIT appears in RUES
    logger.info("creating RUES match column")
    rues_nits = list(set(rues_df["numero_de_identificacion"].astype(str)))
    zasca_df["zasca_and_rues"] = zasca_df["nit"].astype(str).isin(rues_nits)

    # save enhanced ZASCA dataset
    output_dir = Path(DATA_DIR) / "02_processed"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"zasca_{sector}_total.csv"

    logger.info("saving processed ZASCA data to %s", output_path)
    zasca_df.to_csv(output_path, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process ZASCA data and create a unified CSV file.")
    parser.add_argument(
        "--sector",
        choices=["manufactura", "agro"],
        default="manufactura",
        help="Sector to filter by: manufactura (GRUPOS12=3) or agro (GRUPOS12=1) (default: manufactura)",
    )

    args = parser.parse_args()
    main(args.sector)
