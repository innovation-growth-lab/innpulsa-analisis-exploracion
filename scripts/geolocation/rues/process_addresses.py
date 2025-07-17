#!/usr/bin/env python3
"""Process RUES commercial addresses via Gemini LLM to standardise street names.

python scripts/geolocation/rues/process_addresses.py --prompt rues
"""

import asyncio
import sys
import argparse
from pathlib import Path

from innpulsa.processing.rues import read_processed_rues
from innpulsa.loaders import load_csv
from innpulsa.geolocation.address_processor import AddressProcessor
from innpulsa.settings import DATA_DIR
from innpulsa.logging import configure_logger

logger = configure_logger("geolocation.rues_llm")


async def run_pipeline(prompt: str) -> int:
    """Run the RUES address processing pipeline."""
    # Initialise shared processor for RUES dataset
    processor = AddressProcessor("rues")

    # Load data
    rues_df = read_processed_rues()
    zasca_geocoded = load_csv(
        Path(DATA_DIR) / "processed/geolocation/zasca_addresses.csv",
        encoding="utf-8-sig",
    )

    # Select only rues_df observations in source_year 2023
    rues_df = rues_df[rues_df["source_year"] == 2023]

    # Process addresses using shared processor
    results_df = await processor.process_addresses(
        df=rues_df, prompt=prompt, filter_against_zasca=zasca_geocoded, target_n=520
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
        description="Process RUES addresses with Gemini LLM and standardise street names",
    )
    parser.add_argument(
        "--prompt",
        choices=["rues"],
        default="rues",
        help="LLM prompt to use (default: rues)",
    )

    args = parser.parse_args()
    sys.exit(asyncio.run(run_pipeline(args.prompt)))
