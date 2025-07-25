#!/usr/bin/env python3
"""Merge ZASCA with Google and Nominatim coordinates and compare them.

The script expects the following files (generated by other scripts):
    processed/geolocation/zasca_addresses.csv → original cleaned addresses
    processed/geolocation/zasca_coordinates.csv → results Google Maps
    processed/geolocation/zasca_coordinates_nominatim.csv → results Nominatim

Output: a CSV with the merged data plus a boolean `coord_match` column that is
True when both latitude and longitude differ by less than a small tolerance.
"""

import sys
from pathlib import Path

from innpulsa.loaders import load_csv
from innpulsa.settings import DATA_DIR
from innpulsa.logging import configure_logger

# tolerance in degrees for considering two coordinates equal
TOLERANCE = 1e-4


def main() -> int:
    """
    Merge the three datasets and compare coordinates.

    This script will:
    - Load the ZASCA addresses
    - Load the Google Maps coordinates
    - Load the Nominatim coordinates
    - Merge the three datasets
    - Compare the coordinates
    - Save the merged data

    Args:
        None

    Returns:
        int: 0 if successful, 1 otherwise.

    """
    logger = configure_logger("geolocation.compare")

    base_dir = Path(DATA_DIR) / "processed/geolocation"
    try:
        addr_df = load_csv(
            base_dir / "zasca_addresses.csv", encoding="utf-8-sig"
        )
        google_df = load_csv(base_dir / "zasca_coordinates.csv")
        nom_df = load_csv(base_dir / "zasca_coordinates_nominatim.csv")
    except FileNotFoundError:
        logger.exception("Required file not found: %s")
        return 1

    # rename columns for clarity before merging
    google_df = google_df.rename(
        columns={
            "latitude": "google_lat",
            "longitude": "google_lng",
            "gmaps_address": "google_address",
        }
    )
    nom_df = nom_df.rename(
        columns={
            "latitude": "nom_lat",
            "longitude": "nom_lng",
            "nominatim_address": "nom_address",
        }
    )

    # merge
    merged = addr_df.merge(
        google_df[["id", "google_lat", "google_lng", "google_address"]],
        on="id",
        how="left",
    )
    merged = merged.merge(
        nom_df[["id", "nom_lat", "nom_lng", "nom_address"]],
        on="id",
        how="left",
    )

    # compare coordinates
    lat_close = (merged["google_lat"] - merged["nom_lat"]).abs() < TOLERANCE
    lng_close = (merged["google_lng"] - merged["nom_lng"]).abs() < TOLERANCE

    merged["coord_match"] = (
        merged[["google_lat", "google_lng", "nom_lat", "nom_lng"]]
        .notna()
        .all(axis=1)
        & lat_close
        & lng_close
    )

    match_count = merged["coord_match"].sum()
    logger.info(
        "%d/%d addresses have matching coordinates (±%f deg)",
        match_count,
        len(merged),
        TOLERANCE,
    )

    out_path = base_dir / "zasca_coordinates_comparison.csv"
    merged.to_csv(out_path, index=False)
    logger.info("Merged comparison saved to %s", out_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
