"""innpulsa.loaders

Standardised helper functions for loading reference datasets and raw files so
other modules don't have to repeat boilerplate like opening JSON, CSV, etc.
"""

from __future__ import annotations

import pandas as pd

from .generic import load_json, load_csv, load_stata
from .rues import load_rues
from .zasca import load_zasca

__all__ = ["load_rues", "load_zasca", "load_json", "load_csv", "load_stata"]


# Specific loaders
def load_zipcodes_co(as_dataframe: bool = False):
    """Load Colombian postcode reference table.

    Parameters
    ----------
    as_dataframe : bool (default False)
        If True return a pandas DataFrame; else return the raw list of dicts.
    """
    data = load_json("data/01_raw/zipcodes.co.json")
    return pd.DataFrame(data) if as_dataframe else data
