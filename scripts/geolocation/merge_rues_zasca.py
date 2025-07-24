"""
Script to merge RUES and ZASCA data.

This script reads data from both RUES and ZASCA sources and performs an inner merge
based on business identifiers.
"""

from pathlib import Path
import pandas as pd

from innpulsa.settings import DATA_DIR
from innpulsa.logging import configure_logger

logger = configure_logger("geolocation.merge_rues_zasca")


def main():
    """
    Merge RUES and ZASCA data.

    This script will:
    - Read the RUES and ZASCA data
    - Merge the data
    - Save the results

    Args:
        None

    """
    # read data from both sources
    rues_total = pd.read_csv(str(Path(DATA_DIR) / "processed/rues_total.csv"), encoding="utf-8-sig", low_memory=False)
    rues_coords = pd.read_csv(str(Path(DATA_DIR) / "processed/geolocation/rues_coordinates.csv"), encoding="utf-8-sig")
    zasca_addresses = pd.read_csv(
        str(Path(DATA_DIR) / "processed/geolocation/zasca_addresses.csv"), encoding="utf-8-sig"
    )

    rues_total = rues_total.copy()
    # filter rues_total to only include rows in rues_coords
    rues_filtered = rues_total[
        rues_total["nit"].isin(rues_coords["id"].astype(str))
        | rues_total["nit"].isin(zasca_addresses["nit"].astype(str))
    ].query("source_year == 2023")

    # merge in_rues column from zasca_addresses to rues_total
    rues_filtered = rues_filtered.merge(
        zasca_addresses[["nit", "in_rues"]],
        on="nit",
        how="left",
    )

    # merge data
    rues_filtered["in_rues"] = rues_filtered["in_rues"].fillna(value=False).infer_objects(copy=False)

    # save to csv
    rues_filtered.to_csv(DATA_DIR / "processed/geolocation/rues_total_merged.csv", encoding="utf-8-sig")


if __name__ == "__main__":
    main()
