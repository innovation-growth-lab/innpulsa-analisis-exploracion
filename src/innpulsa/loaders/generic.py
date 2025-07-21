"""Generic loader functions for common file formats."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd
import pyreadstat as prs

from ..settings import DATA_DIR


def _project_path(path: Union[str, Path]) -> Path:
    """Resolve a path relative to the repo root unless it is already absolute."""
    p = Path(path)
    return p if p.is_absolute() else (Path(DATA_DIR).parent / p).resolve()


def load_json(path: Union[str, Path]) -> List[Dict[str, Any]]:
    """Load a JSON file from an absolute or project-relative path."""
    with open(_project_path(path), "r", encoding="utf-8") as fp:
        return json.load(fp)


def load_csv(path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """Shortcut around `pd.read_csv` for absolute or project-relative paths."""
    return pd.read_csv(_project_path(path), **kwargs)


def load_stata(
    path: Union[str, Path], pyreadstat: bool = True, **kwargs
) -> pd.DataFrame:
    """Load Stata file (.dta) using `pyreadstat` (faster) from absolute or project path."""
    if pyreadstat:
        df, _ = prs.read_dta(str(_project_path(path)), **kwargs)
    else:
        df = pd.read_stata(str(_project_path(path)), **kwargs)
    return df
