"""
ZASCA data loading module.

This module handles the loading of ZASCA data from multiple cohorts
"""

import logging
from pathlib import Path
import pandas as pd
from innpulsa.settings import RAW_DATA_DIR, DATA_DIR
from .generic import load_stata, load_csv

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
]

logger = logging.getLogger("innpulsa.loaders.zasca")


def load_zasca() -> pd.DataFrame:
    """
    Read and process ZASCA data from multiple cohort files.

    Returns:
        DataFrame

    Raises:
        ValueError: if no cohort files could be read

    """
    # define cohort files
    cohort_files = {
        "CUCC2": Path(RAW_DATA_DIR) / "Zasca_CUC_C2.dta",
        "MEDC1": Path(RAW_DATA_DIR) / "Zasca_MED_C1.dta",
        "MEDC2": Path(RAW_DATA_DIR) / "Zasca_MED_C2.dta",
        "BMAC1": Path(RAW_DATA_DIR) / "Zasca_BMA_C1.dta",
        "CUCC1": Path(RAW_DATA_DIR) / "Zasca_CUC_C1.dta",
    }

    # read and combine all cohorts
    dfs = []
    for cohort_name, file_path in cohort_files.items():
        try:
            logger.debug("reading cohort file: %s", file_path)
            df = load_stata(file_path, encoding="utf-8")

            # select only relevant columns before processing
            df = select_relevant_columns(df, df.columns.tolist())

            df["cohort"] = cohort_name  # add cohort identifier
            dfs.append(df)
        except Exception:
            logger.exception("failed to read %s", file_path)
            continue

    if not dfs:
        logger.error("no cohort files were successfully read")
        raise ValueError

    # combine all cohorts and process
    logger.debug("combining and processing cohort data")
    return pd.concat(dfs, ignore_index=True)


def load_processed_zasca() -> pd.DataFrame:
    """Read the pre-processed ZASCA data from CSV.

    Returns:
        pd.DataFrame: Processed ZASCA data from saved CSV file.

    """
    zasca_path = Path(DATA_DIR) / "processed/zasca_total.csv"
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
    zasca_addresses_path = Path(DATA_DIR) / "processed/geolocation/zasca_addresses.csv"
    logger.info("reading ZASCA addresses from %s", zasca_addresses_path)
    return load_csv(zasca_addresses_path, encoding="utf-8-sig")
