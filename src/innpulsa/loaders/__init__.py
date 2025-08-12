"""Standardised helper functions for loading reference datasets and raw files."""

from __future__ import annotations

from typing import Any
import pandas as pd

from .generic import load_json, load_csv, load_stata
from .rues import load_rues, load_processed_rues
from .zasca import load_processed_zasca, load_zasca_addresses, load_zascas

__all__ = [
    "load_zascas",
    "load_csv",
    "load_json",
    "load_processed_rues",
    "load_processed_zasca",
    "load_rues",
    "load_stata",
    "load_zasca_addresses",
]


# Specific loaders
def load_zipcodes_co(*, as_dataframe: bool = False) -> pd.DataFrame | list[dict[str, Any]]:
    """
    Load Colombian postcode reference table.

    Args:
        as_dataframe: if True return a pandas DataFrame; else return the raw list of dicts

    Returns:
        DataFrame or list of dicts

    """
    data = load_json("data/01_raw/zipcodes.co.json")
    return pd.DataFrame(data) if as_dataframe else data
