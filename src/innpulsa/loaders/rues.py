"""
RUES data loading module.

This module handles the loading of RUES (Registro Único Empresarial y Social) data.
"""

from pathlib import Path
import logging
import pandas as pd
from ..settings import RAW_DATA_DIR, DATA_DIR
from .generic import load_stata, load_csv

logger = logging.getLogger("innpulsa.loaders.rues")


def load_rues() -> pd.DataFrame:
    """Read and combine RUES data from multiple years (raw)."""

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
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("failed to read %s: %s", file_path, exc)

    if not dfs:
        raise ValueError("No RUES files could be read.")

    return pd.concat(dfs, ignore_index=True)


def load_processed_rues() -> pd.DataFrame:
    """Load the saved combined RUES CSV."""

    path = Path(DATA_DIR) / "processed/rues_total.csv"
    logger.info("reading processed RUES data from %s", path)
    return load_csv(path, encoding="utf-8-sig", low_memory=False)
