"""
RUES data processing module.

This module handles the processing of RUES (Registro Único Empresarial y Social) data,
including reading and combining data from multiple years.
"""

from pathlib import Path
import pandas as pd
from ..settings import RAW_DATA_DIR


def read_rues() -> pd.DataFrame:
    """Read and combine RUES data from multiple years.

    Returns:
        pd.DataFrame: Combined DataFrame containing RUES data from 2023 and 2024.
    """
    rues_dir = Path(RAW_DATA_DIR) / "Rues"

    files = {
        2023: rues_dir / "Activas y renovadas 2023-marz2024.dta",
        2024: rues_dir / "Activas y renovadas 2024-marz2025.dta",
    }

    # Read and combine all RUES data files
    dfs = []
    for year, file_path in files.items():
        df = pd.read_stata(file_path)
        df["source_year"] = year
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)
