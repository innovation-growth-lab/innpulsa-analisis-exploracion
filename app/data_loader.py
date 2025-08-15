"""Data loader for the application."""

from __future__ import annotations
import unicodedata
import pandas as pd

from constants import DATA_WITH_COORDS_PATH


def normalise_str(s: str) -> str:
    """
    Return lowercase ASCII-only version of *s* (remove accents).

    Args:
        s: The string to normalise

    Returns:
        str: The normalised string

    """
    return (
        unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower() if isinstance(s, str) else ""
    )


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load, merge and clean the data used by the application.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            data_with_coords, top_3_ciiu_principal

    """
    data_with_coords = pd.read_csv(DATA_WITH_COORDS_PATH, encoding="utf-8-sig")

    # drop all columns containing '2024'
    data_with_coords = data_with_coords.loc[:, ~data_with_coords.columns.str.contains("2024")]

    # remove '_2023' suffix from remaining columns
    data_with_coords.columns = data_with_coords.columns.str.replace(r"_2023$", "", regex=True)

    # identify the top 3 ciiu_principal per centro
    top_3_ciiu_principal = (
        data_with_coords.groupby(["ciiu_principal", "centro"])  # type: ignore[arg-type]
        .size()
        .reset_index(name="count")
        .sort_values(["centro", "count"], ascending=[True, False])
        .groupby("centro")
        .head(3)
    )

    top_3_ciiu_principal["ciiu_principal"] = top_3_ciiu_principal["ciiu_principal"].astype(int)

    # convert lat / lon to numeric and drop NAs
    data_with_coords["latitude"] = pd.to_numeric(data_with_coords["latitude"], errors="coerce")
    data_with_coords["longitude"] = pd.to_numeric(data_with_coords["longitude"], errors="coerce")
    data_with_coords = data_with_coords.dropna(subset=["latitude", "longitude"])

    # define variables for "rues", "zasca", "zasca_and_rues"
    data_with_coords["rues"] = ~data_with_coords["activos_total"].isna()
    data_with_coords["zasca"] = ~data_with_coords["id"].isna()
    data_with_coords["zasca_and_rues"] = data_with_coords["rues"] & data_with_coords["zasca"]
    data_with_coords["zasca_only"] = data_with_coords["zasca"] & ~data_with_coords["rues"]
    data_with_coords["rues_only"] = data_with_coords["rues"] & ~data_with_coords["zasca"]

    return data_with_coords, top_3_ciiu_principal
