"""
ZASCA data loading module.

This module handles the loading of ZASCA data from multiple cohorts
"""

import logging
from pathlib import Path
import pandas as pd
from innpulsa.settings import RAW_DATA_DIR, DATA_DIR
from .generic import load_csv

# define relevant columns to keep from ZASCA data
ZASCA_RELEVANT_COLUMNS = [
    "numberid_emp1",
    "nit",
    "city",
    "barrio",
    "vereda",
    "address",
    "estab_firm",
    "sex_emp1",
    "sales2020s",
    "sales2021s",
    "sales2022s",
    "sales2023q1s",
    "sales2024q1s",
    "salesaverage2024",
    "weeklysales",
    "emp_total",
    "capital",
    "cohort",
    "centro",
    "Cierre",

    # descriptive variables
    "yearcohort",
    "birth_emp1",
    "sex_emp1",
    "sisben_emp1",
    "headhousehold",
    "householdcare",
    "dpto",
    "zona",
    "yearsales",
    "sales2023",  # many NA
    "emp_ftc",
    "emp_htc",
    "emp_psc",
    "emp_volc",
    "emp_internc",
    "reason2start",
    "rut",
    "bookkeeping",
    "hascredit"
]

logger = logging.getLogger("innpulsa.loaders.zasca")


def load_zascas() -> pd.DataFrame:
    """Read the closed ZASCA data from CSV.

    Returns:
        pd.DataFrame: Closed ZASCA data from saved CSV file.

    """
    closed_zascas = pd.read_csv(Path(RAW_DATA_DIR) / "Zascas_cerrados.csv", encoding="utf-8-sig")
    closed_zascas = select_relevant_columns(closed_zascas, closed_zascas.columns.tolist())
    closed_zascas["cohort"] = closed_zascas["cohort"].astype(str) + closed_zascas["centro"].astype(str)

    return closed_zascas


def load_processed_zasca() -> pd.DataFrame:
    """Read the pre-processed ZASCA data from CSV.

    Returns:
        pd.DataFrame: Processed ZASCA data from saved CSV file.

    """
    zasca_path = Path(DATA_DIR) / "02_processed/zasca_total.csv"
    logger.info("reading processed ZASCA data from %s", zasca_path)

    try:
        df = load_csv(zasca_path, encoding="utf-8-sig")

        logger.debug("successfully read %d ZASCA records", len(df))

    except Exception:
        logger.exception("failed to read processed ZASCA data")
        raise
    else:
        return df


def select_relevant_columns(df: pd.DataFrame, available_columns: list) -> pd.DataFrame:
    """Select only relevant columns that exist in the dataframe.

    Args:
        df: Input DataFrame
        available_columns: List of column names available in df

    Returns:
        DataFrame with only relevant columns

    """
    # find intersection of relevant columns and available columns
    columns_to_keep = [col for col in ZASCA_RELEVANT_COLUMNS if col in available_columns]

    if not columns_to_keep:
        logger.warning("no relevant columns found in dataframe")
        return df

    logger.debug("keeping %d relevant columns: %s", len(columns_to_keep), columns_to_keep)
    result = df[columns_to_keep].copy()
    return result if isinstance(result, pd.DataFrame) else result.to_frame()


def load_zasca_addresses() -> pd.DataFrame:
    """Read the pre-processed ZASCA addresses from CSV.

    Returns:
        pd.DataFrame: Processed ZASCA addresses from saved CSV file.

    """
    zasca_addresses_path = Path(DATA_DIR) / "02_processed/geolocation/zasca_addresses.csv"

    logger.info("reading ZASCA addresses from %s", zasca_addresses_path)
    return load_csv(zasca_addresses_path, encoding="utf-8-sig")
