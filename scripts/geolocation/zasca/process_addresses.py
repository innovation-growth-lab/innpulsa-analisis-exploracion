#!/usr/bin/env python3
"""Process ZASCA addresses with LLM and save results."""

import asyncio
import sys

from innpulsa.loaders import load_processed_zasca
from innpulsa.geolocation.address_processor import AddressProcessor
from innpulsa.logging import configure_logger
from innpulsa.geolocation.prompts import SYSTEM_PROMPT_ZASCA


async def main() -> int:
    """
    Process ZASCA addresses and save results.

    This script will:
    - Load the ZASCA data
    - Process the ZASCA data
    - Save the processed ZASCA data

    Args:
        target_n: The number of addresses to process.
        clear_existing: Whether to clear existing batch files.

    Returns:
        int: 0 if successful, 1 otherwise.

    """
    logger = configure_logger("zasca_llm")

    # Initialise shared processor for ZASCA dataset
    processor = AddressProcessor("zasca")

    # Load input data
    logger.info("loading ZASCA data")
    df = load_processed_zasca()
    if df is None:
        logger.error("failed to load ZASCA data")
        return 1

    logger.info("Starting ZASCA address processing")
    results_df = await processor.process_addresses(df, SYSTEM_PROMPT_ZASCA)

    if results_df is None:
        logger.error("No results were generated")
        return 1

    logger.info("Successfully processed %d addresses", len(results_df))

    # Merge the in_rues column (ZASCA-specific post-processing)
    results_df["id"] = results_df["id"].astype(int)
    results_df = results_df.merge(
        df[["numberid_emp1", "nit", "in_rues"]],
        left_on="id",
        right_on="numberid_emp1",
        how="inner",
    )

    # Drop numberid_emp1 duplicates
    results_df = results_df.drop_duplicates(subset=["numberid_emp1"])
    results_df = results_df.drop(columns=["numberid_emp1"])

    # Save results
    output_path = processor.save_results(results_df)
    logger.info(
        "Successfully processed %d ZASCA addresses and saved to %s",
        len(results_df),
        output_path,
    )

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
