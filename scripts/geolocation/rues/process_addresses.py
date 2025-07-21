#!/usr/bin/env python3
"""Process RUES commercial addresses via Gemini LLM to standardise street names.

python scripts/geolocation/rues/process_addresses.py --target 520
python scripts/geolocation/rues/process_addresses.py --clear --target 520  # Clear existing files first
"""

import asyncio
import sys
import argparse

from innpulsa.logging import configure_logger
from innpulsa.loaders import load_zasca_addresses, load_processed_rues
from innpulsa.geolocation.prompts import SYSTEM_PROMPT_RUES
from innpulsa.geolocation.address_processor import AddressProcessor

logger = configure_logger("geolocation.rues_llm")


async def run_pipeline(target_n: int, clear_existing: bool = False) -> int:
    """Run the RUES address processing pipeline."""
    # Initialise shared processor for RUES dataset
    processor = AddressProcessor("rues")

    if clear_existing:
        logger.info("Clearing existing batch files from %s", processor.output_dir)
        batch_files = list(processor.output_dir.glob("batch_*"))
        for batch_file in batch_files:
            batch_file.unlink()
            logger.debug("Deleted %s", batch_file)
        logger.info("Cleared %d existing batch files", len(batch_files))

    # Load data
    rues_df = load_processed_rues()
    zasca_addresses = load_zasca_addresses()

    # Select only rues_df observations in source_year 2023
    rues_df = rues_df[rues_df["source_year"] == 2023]

    # Process addresses using shared processor
    results_df = await processor.process_addresses(
        df=rues_df,
        prompt=SYSTEM_PROMPT_RUES,
        filter_against_zasca=zasca_addresses,
        target_n=target_n,
    )

    if results_df is None:
        logger.error("No results were generated")
        return 1

    # Save results
    output_path = processor.save_results(results_df)
    logger.info(
        "Successfully processed %d RUES addresses and saved to %s",
        len(results_df),
        output_path,
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process RUES commercial addresses via Gemini LLM to standardise street names."
    )
    parser.add_argument(
        "--target",
        type=int,
        default=520,
        help="Number of addresses to process (default: 520)",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Delete all existing batch files before processing",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(run_pipeline(args.target, args.clear)))
