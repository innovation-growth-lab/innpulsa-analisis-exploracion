"""Generic loader functions for common file formats."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyreadstat as prs

from innpulsa.settings import DATA_DIR


def _project_path(path: str | Path) -> Path:
    """
    Resolve a path relative to the repo root unless it is already absolute.

    Args:
        path: path to the file

    Returns:
        resolved path

    """
    p = Path(path)
    return p if p.is_absolute() else (Path(DATA_DIR).parent / p).resolve()


def load_json(path: str | Path) -> list[dict[str, Any]]:
    """
    Load a JSON file from an absolute or project-relative path.

    Args:
        path: path to the file

    Returns:
        list of dicts

    """
    with Path(_project_path(path)).open("r", encoding="utf-8") as fp:
        return json.load(fp)


def load_csv(path: str | Path, **kwargs) -> pd.DataFrame:
    """
    Shortcut around `pd.read_csv` for absolute or project-relative paths.

    Args:
        path: path to the file
        kwargs: additional keyword arguments to pass to `pd.read_csv`

    Returns:
        DataFrame

    """
    return pd.read_csv(_project_path(path), **kwargs)


def load_stata(path: str | Path, *, pyreadstat: bool = True, **kwargs) -> pd.DataFrame:
    """
    Load Stata file (.dta) using `pyreadstat` (faster) from absolute or project path.

    Args:
        path: path to the file
        pyreadstat: if True use `pyreadstat` to read the file; else use `pandas.read_stata`
        kwargs: additional keyword arguments to pass to `pyreadstat.read_dta` or `pandas.read_stata`

    Returns:
        DataFrame

    """
    if pyreadstat:
        df, _ = prs.read_dta(str(_project_path(path)), **kwargs)
    else:
        df = pd.read_stata(str(_project_path(path)), **kwargs)
    return pd.DataFrame(df)
