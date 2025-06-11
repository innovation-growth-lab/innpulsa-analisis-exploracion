#!/usr/bin/env python3
"""Process ZASCA addresses with LLM and save results."""

import asyncio
import os
import sys

from innpulsa.processing.zasca import read_processed_zasca
from innpulsa.geolocation.processing import GeolocationProcessor
from innpulsa.logging import configure_logger


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
        output_file, processed_count = await processor.process_and_compile(df)

        if output_file:
            logger.info("Successfully processed %d addresses", processed_count)
            logger.info("Results saved to: %s", output_file)
            return 0

        logger.error("No results were generated")
        return 1

    except Exception:  # pylint: disable=W0718
        logger.exception("Failed to process addresses")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
