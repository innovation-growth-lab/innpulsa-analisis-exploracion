"""geocode processed addresses using Google's Geocoding API or Nominatim.

Usage:
    python scripts/geolocation/geocode_addresses.py --service nominatim --dataset zasca
    python scripts/geolocation/geocode_addresses.py --service google --dataset rues
"""

import asyncio
import os
import sys
import argparse
from pathlib import Path
import pandas as pd
from tqdm import tqdm as tqdm_sync
from geopy.geocoders import Nominatim
from typing import Any
from geopy.exc import GeocoderServiceError

from innpulsa.settings import DATA_DIR
from innpulsa.geolocation.geocoding import GoogleGeocoder
from innpulsa.logging import configure_logger
from innpulsa.loaders import load_csv

TIMEOUT = 60


async def google_geocode(dataset: str) -> int:
    """
    Geocode addresses using the Google Maps API (async).

    This script will:
    - Load the processed addresses
    - Geocode the addresses using the Google Maps API
    - Save the results

    Args:
        dataset: The dataset to geocode (zasca or rues)

    Returns:
        int: 0 if successful, 1 otherwise.

    """
    logger = configure_logger("geolocation.geocoding")

    # check API key
    api_key = os.getenv("GMAPS_API_KEY")
    if not api_key:
        logger.error("GMAPS_API_KEY environment variable is required")
        return 1

    # load processed addresses
    try:
        input_path = Path(DATA_DIR) / f"02_processed/geolocation/{dataset}_addresses.csv"
        logger.info("reading addresses from %s", input_path)
        df = load_csv(input_path, encoding="utf-8-sig")
    except Exception:  # pylint: disable=broad-except
        logger.exception("failed to read input file")
        return 1

    # prepare addresses for geocoding
    addresses = {
        str(row["id" if "id" in df.columns else "nit"]): {
            "formatted_address": str(row["formatted_address"]),
            "country": str(row["country"]),
            "area": str(row["area"]),
            "city": str(row["city"]),
        }
        for _, row in df.iterrows()
    }

    # initialise geocoder and process addresses
    checkpoint_path = Path(DATA_DIR) / f"02_processed/geolocation/{dataset}_coordinates_checkpoint.json"

    async with GoogleGeocoder(api_key) as geocoder:
        logger.info("starting geocoding for %d addresses", len(addresses))
        results: dict[str, dict[str, Any]] = await geocoder.geocode_batch(
            addresses,
            checkpoint_path=checkpoint_path,
        )

    # convert results to dataframe
    output_records = []
    for id_, result in results.items():
        output_records.append({
            "id": id_,
            "gmaps_address": result["gmaps_address"],
            "latitude": result["coords"][0] if result["coords"] else None,
            "longitude": result["coords"][1] if result["coords"] else None,
        })

    # save results
    output_df = pd.DataFrame(output_records)
    output_path = Path(DATA_DIR) / f"02_processed/geolocation/{dataset}_coordinates.csv"
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


def nominatim_geocode(dataset: str) -> int:
    """
    Geocode addresses using OpenStreetMap's Nominatim (sync).

    This script will:
    - Load the processed addresses
    - Geocode the addresses using Nominatim
    - Save the results

    Args:
        dataset: The dataset to geocode (zasca or rues)

    Returns:
        int: 0 if successful, 1 otherwise.

    """
    logger = configure_logger("geolocation.geocoding.nominatim")

    # load processed addresses (no need for API key)
    input_path = Path(DATA_DIR) / f"02_processed/geolocation/{dataset}_addresses.csv"
    logger.info("reading addresses from %s", input_path)
    df = load_csv(input_path, encoding="utf-8-sig")

    addresses = {
        str(row["id" if "id" in df.columns else "nit"]): {
            "formatted_address": str(row["formatted_address"]),
            "country": str(row["country"]),
            "area": str(row["area"]),
            "city": str(row["city"]),
        }
        for _, row in df.iterrows()
    }

    # geocode with geopy directly (no need to instantiate GoogleGeocoder)
    geolocator = Nominatim(user_agent="innpulsa-geocoder@nesta.org.uk")
    results: dict[str, tuple[str, tuple[float, float]] | None] = {}
    logger.info("starting Nominatim geocoding for %d addresses", len(addresses))
    for id_, addr in tqdm_sync(addresses.items(), desc="Nominatim", total=len(addresses)):
        query = f"{addr['formatted_address']}, {addr['country']}, {addr['area']}, {addr['city']}"
        try:
            location = geolocator.geocode(query)
            results[id_] = (
                None
                if location is None
                else (
                    location.address,
                    (location.latitude, location.longitude),
                )
            )
        except (GeocoderServiceError, ValueError) as exc:
            logger.warning("Nominatim error for %s: %s", id_, exc)
            results[id_] = None

    output_records = []
    for id_, result in results.items():
        output_records.append({
            "id": id_,
            "nominatim_address": result[0] if result else None,
            "latitude": result[1][0] if result and result[1] else None,
            "longitude": result[1][1] if result and result[1] else None,
        })

    output_df = pd.DataFrame(output_records)
    output_path = Path(DATA_DIR) / f"02_processed/geolocation/{dataset}_coordinates_nominatim.csv"
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
    parser = argparse.ArgumentParser(description="Geocode ZASCA addresses using Google Maps or Nominatim.")
    parser.add_argument(
        "--service",
        choices=["google", "nominatim"],
        default="nominatim",
        help="Geocoding service to use (default: nominatim)",
    )
    parser.add_argument(
        "--dataset",
        choices=["zasca", "rues"],
        default="zasca",
        help="Which dataset addresses to geocode (default: zasca)",
    )

    args = parser.parse_args()

    if args.service == "google":
        sys.exit(asyncio.run(google_geocode(args.dataset)))
    else:
        sys.exit(nominatim_geocode(args.dataset))
