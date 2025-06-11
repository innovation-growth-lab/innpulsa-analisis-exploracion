#!/usr/bin/env python3
"""Process ZASCA addresses with LLM and save results."""

import asyncio
import os
import sys
from pathlib import Path

from innpulsa.processing.zasca import read_processed_zasca
from innpulsa.geolocation.processing import GeolocationProcessor
from innpulsa.logging import configure_logger
from innpulsa.settings import DATA_DIR
from innpulsa.geolocation.prompts import SYSTEM_PROMPT_ZASCA


async def main() -> int:
    """Process addresses and save results."""
    logger = configure_logger("geolocation")

    if not os.getenv("GEMINI_API_KEY"):
        logger.error("GEMINI_API_KEY environment variable is required")
        return 1

    # load input data
    logger.info("loading ZASCA data")
    df = read_processed_zasca()
    if df is None:
        logger.error("failed to load ZASCA data")
        return 1

    processor = GeolocationProcessor()
    try:
        logger.info("Starting ZASCA address processing")
        results_df = await processor.process_and_compile(df, SYSTEM_PROMPT_ZASCA)

        if results_df is not None:
            logger.info("Successfully processed %d addresses", len(results_df))

            # merge the in_rues column
            results_df["id"] = results_df["id"].astype(int)

            results_df = results_df.merge(
                df[["numberid_emp1", "nit", "in_rues"]],
                left_on="id",
                right_on="numberid_emp1",
                how="inner",
            )

            # drop numberid_emp1 duplicates
            results_df = results_df.drop_duplicates(subset=["numberid_emp1"])

            # drop numberid_emp1 col
            results_df = results_df.drop(columns=["numberid_emp1"])

            logger.info("Saving results to CSV")
            output_file = Path(DATA_DIR) / "processed/geolocation/zasca_addresses.csv"
            output_file.parent.mkdir(parents=True, exist_ok=True)
            results_df.to_csv(output_file, index=False, encoding="utf-8-sig")
            logger.info("saved %d records to %s", len(results_df), output_file)
            return 0

        logger.error("No results were generated")
        return 1

    except Exception:  # pylint: disable=W0718
        logger.exception("Failed to process addresses")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
