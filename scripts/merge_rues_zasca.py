"""
Script to merge RUES and ZASCA data.

This script reads data from both RUES and ZASCA sources and performs an inner merge
based on business identifiers.
"""

import logging
from pathlib import Path
import pandas as pd
from innpulsa.processing.rues import read_processed_rues
from innpulsa.processing.zasca import read_processed_zasca

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def main():
    """Main function to merge RUES and ZASCA data."""
    # read data from both sources
    logger.info("reading processed RUES data")
    rues_df = read_processed_rues()

    logger.info("reading processed ZASCA data")
    zasca_df = read_processed_zasca()

    # ensure nit columns are clean
    logger.info("ensuring NIT columns are clean")

    if zasca_df["nit"].dtype != "object":
        zasca_df["nit"] = zasca_df["nit"].astype(str)
    zasca_df["nit"] = (
        zasca_df["nit"].replace("nan", "").str.replace(r"\.0$", "", regex=True)
    )

    # perform inner merge
    logger.info("merging datasets")
    merged_df = pd.merge(
        rues_df,
        zasca_df,
        on="nit",
        how="inner",
        # validate="1:1",  # ensure one-to-one merge
    )

    # log merge statistics
    logger.info(
        "merge statistics: RUES records=%d, ZASCA records=%d, matched records=%d, "
        "match rate=%.2f%%",
        len(rues_df),
        len(zasca_df),
        len(merged_df),
        len(merged_df) / len(zasca_df) * 100,
    )

    # save merged data
    output_path = Path("data/processed/rues_zasca_merged.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("saving merged data to %s", output_path)
    merged_df.to_csv(output_path, index=False)
    logger.info("processing completed successfully")


if __name__ == "__main__":
    main()
