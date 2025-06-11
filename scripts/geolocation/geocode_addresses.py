#!/usr/bin/env python3
"""geocode processed ZASCA addresses using Google's Geocoding API."""

import asyncio
import os
import sys
from pathlib import Path
import pandas as pd
import argparse

from innpulsa.settings import DATA_DIR
from innpulsa.geolocation.geocoding import GoogleGeocoder
from innpulsa.logging import configure_logger


async def google_geocode() -> int:
    """Geocode addresses using the Google Maps API (async)."""
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


def nominatim_geocode() -> int:
    """Geocode addresses using OpenStreetMap's Nominatim (sync)."""
    logger = configure_logger("geolocation.geocoding.nominatim")

    # load processed addresses (no need for API key)
    input_path = Path(DATA_DIR) / "processed/geolocation/zasca_addresses.csv"
    logger.info("reading addresses from %s", input_path)
    df = pd.read_csv(input_path, encoding="latin1")

    addresses = {
        row["id"]: {
            "formatted_address": row["formatted_address"],
            "country": row["country"],
            "area": row["area"],
            "city": row["city"],
        }
        for _, row in df.iterrows()
    }

    # reuse helper already defined in GoogleGeocoder for convenience
    geocoder = GoogleGeocoder("")  # dummy key, not used for Nominatim
    logger.info("starting Nominatim geocoding for %d addresses", len(addresses))
    results = geocoder.geocode_with_nominatim_batch(addresses)

    output_records = []
    for id_, result in results.items():
        output_records.append(
            {
                "id": id_,
                "nominatim_address": result[0] if result else None,
                "latitude": result[1][0] if result and result[1] else None,
                "longitude": result[1][1] if result and result[1] else None,
            }
        )

    output_df = pd.DataFrame(output_records)
    output_path = (
        Path(DATA_DIR) / "processed/geolocation/zasca_coordinates_nominatim.csv"
    )
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
    parser = argparse.ArgumentParser(
        description="Geocode ZASCA addresses using Google Maps or Nominatim."
    )
    parser.add_argument(
        "--service",
        choices=["google", "nominatim"],
        default="google",
        help="Geocoding service to use (default: google)",
    )

    args = parser.parse_args()

    if args.service == "google":
        sys.exit(asyncio.run(google_geocode()))
    else:
        sys.exit(nominatim_geocode())
