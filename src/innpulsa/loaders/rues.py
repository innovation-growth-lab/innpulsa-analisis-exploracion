"""
RUES data loading module.

This module handles the loading of RUES (Registro Único Empresarial y Social) data.
"""

from pathlib import Path
import logging
import pandas as pd
from innpulsa.settings import RAW_DATA_DIR, DATA_DIR
from .generic import load_stata, load_csv

logger = logging.getLogger("innpulsa.loaders.rues")


def load_rues() -> pd.DataFrame:
    """
    Read and combine RUES data from multiple years (raw).

    Returns:
        DataFrame

    Raises:
        ValueError: if no RUES files could be read

    """
    rues_dir = Path(RAW_DATA_DIR) / "Rues"

    files = {
        2023: rues_dir / "Activas y renovadas 2023-marz2024.dta",
        2024: rues_dir / "Activas y renovadas 2024-marz2025.dta",
    }

    dfs = []
    for year, file_path in files.items():
        try:
            logger.debug("reading RUES file: %s", file_path)
            df = load_stata(file_path, pyreadstat=False)
            df["source_year"] = year
            dfs.append(df)
        except Exception:
            logger.exception("failed to read %s", file_path)

    if not dfs:
        raise ValueError

    return pd.concat(dfs, ignore_index=True)


def load_processed_rues() -> pd.DataFrame:
    """
    Load the saved combined RUES CSV.

    Returns:
        DataFrame

    """
    path = Path(DATA_DIR) / "processed/rues_total.csv"
    logger.info("reading processed RUES data from %s", path)
    return load_csv(path, encoding="utf-8-sig", low_memory=False)
