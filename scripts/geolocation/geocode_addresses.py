#!/usr/bin/env python3
"""geocode processed ZASCA addresses using Google's Geocoding API."""

import asyncio
import os
import sys
from pathlib import Path
import pandas as pd

from innpulsa.settings import DATA_DIR
from innpulsa.geolocation.geocoding import GoogleGeocoder
from innpulsa.logging import configure_logger


async def main() -> int:
    """geocode addresses and save results."""
    logger = configure_logger("geolocation.geocoding")

    # check API key
    api_key = os.getenv("GMAPS_API_KEY")
    if not api_key:
        logger.error("GMAPS_API_KEY environment variable is required")
        return 1

    # load processed addresses
    try:
        input_path = Path(DATA_DIR) / "processed/geolocation/zasca_addresses.csv"
        logger.info("reading addresses from %s", input_path)
        df = pd.read_csv(input_path, encoding="latin1")
    except Exception as e:  # pylint: disable=W0718
        logger.error("failed to read input file: %s", str(e))
        return 1

    # prepare addresses for geocoding
    addresses = {
        row["id"]: {
            "formatted_address": row["formatted_address"],
            "country": row["country"],
            "area": row["area"],
            "city": row["city"],
        }
        for _, row in df.iterrows()
    }

    # initialise geocoder and process addresses
    async with GoogleGeocoder(api_key) as geocoder:
        logger.info("starting geocoding for %d addresses", len(addresses))
        results = await geocoder.geocode_batch(addresses)

    # convert results to dataframe
    output_records = []
    for id_, result in results.items():
        output_records.append(
            {
                "id": id_,
                "gmaps_address": result["gmaps_address"],
                "latitude": result["coords"][0] if result["coords"] else None,
                "longitude": result["coords"][1] if result["coords"] else None,
            }
        )

    # save results
    output_df = pd.DataFrame(output_records)
    output_path = Path(DATA_DIR) / "processed/geolocation/zasca_coordinates.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)

    success_count = output_df[["latitude", "longitude"]].notna().all(axis=1).sum()
    logger.info(
        "geocoding completed: %d/%d addresses successfully geocoded",
        success_count,
        len(addresses),
    )
    logger.info("results saved to: %s", output_path)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
