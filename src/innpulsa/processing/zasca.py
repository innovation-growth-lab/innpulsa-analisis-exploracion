"""
ZASCA data processing module.

This module handles the processing of ZASCA data from multiple cohorts,
including reading, cleaning, and combining data.
"""

import os
import logging
from pathlib import Path
import pandas as pd
from ..settings import RAW_DATA_DIR, DATA_DIR
from ..loaders import load_stata, load_csv


logger = logging.getLogger("innpulsa.processing.zasca")


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


def select_relevant_columns(df: pd.DataFrame, available_columns: list) -> pd.DataFrame:
    """Select only relevant columns that exist in the dataframe.

    Args:
        df: Input DataFrame
        available_columns: List of column names available in df

    Returns:
        DataFrame with only relevant columns
    """
    # find intersection of relevant columns and available columns
    columns_to_keep = [
        col for col in ZASCA_RELEVANT_COLUMNS if col in available_columns
    ]

    if not columns_to_keep:
        logger.warning("no relevant columns found in dataframe")
        return df

    logger.debug(
        "keeping %d relevant columns: %s", len(columns_to_keep), columns_to_keep
    )
    return df[columns_to_keep].copy()


def process_zasca_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process raw ZASCA data by standardizing columns and values.

    Args:
        df: Raw ZASCA DataFrame

    Returns:
        Processed DataFrame with standardized columns and values
    """
    # merge barrio and vereda (only one is non-null)
    if "barrio" in df.columns and "vereda" in df.columns:
        df["neighborhood"] = df["barrio"].fillna(df["vereda"])
        df.drop(columns=["barrio", "vereda"], inplace=True)

    # create full_address column
    df["full_address"] = df["address"] + ", " + df["neighborhood"] + ", " + df["city"]

    # standardise text columns
    if "estab_firm" in df.columns:
        df["estab_firm"] = df["estab_firm"].map({"Sí": True, "No": False})

    if "sex_emp1" in df.columns:
        df["female"] = df["sex_emp1"].map({"Masculino": False, "Femenino": True})

    # turn int to str for numberid_emp1 and nit
    if "numberid_emp1" in df.columns:
        df["numberid_emp1"] = df["numberid_emp1"].astype(pd.Int64Dtype()).astype(str)
    if "nit" in df.columns:
        df["nit"] = df["nit"].astype(str)

    # adjust sales data for Bucaramanga units (monthly to quarterly)
    if "cohort" in df.columns:
        mask = df["cohort"] == "BMAC1"
        for col in ["sales2023q1s", "sales2024q1s"]:
            if col in df.columns:
                df.loc[mask, col] = df.loc[mask, col] / 11 * 3

    # impute missing sales2023q1s using weekly sales
    if "sales2023q1s" in df.columns and "weeklysales" in df.columns:
        mask = df["sales2023q1s"].isna()
        df.loc[mask, "sales2023q1s"] = df.loc[mask, "weeklysales"] * 4 * 3

    # convert sales from thousands to millions where needed
    for col in ["sales2023q1s", "sales2024q1s"]:
        if col in df.columns:
            mask = df[col] <= 10000
            df.loc[mask, col] *= 1_000_000

    return df


def read_and_process_zasca(save_processed: bool = True) -> pd.DataFrame:
    """Read and process ZASCA data from multiple cohort files.

    Args:
        save_processed: Whether to save the processed data to CSV

    Returns:
        pd.DataFrame: Combined and processed ZASCA data.
    """

    # define cohort files
    cohort_files = {
        "CUCC2": os.path.join(RAW_DATA_DIR, "Zasca_CUC_C2.dta"),
        "MEDC1": os.path.join(RAW_DATA_DIR, "Zasca_MED_C1.dta"),
        "MEDC2": os.path.join(RAW_DATA_DIR, "Zasca_MED_C2.dta"),
        "BMAC1": os.path.join(RAW_DATA_DIR, "Zasca_BMA_C1.dta"),
        "CUCC1": os.path.join(RAW_DATA_DIR, "Zasca_CUC_C1.dta"),
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
        except Exception as e:  # pylint: disable=W0718
            logger.error("failed to read %s: %s", file_path, str(e))
            continue

    if not dfs:
        logger.error("no cohort files were successfully read")
        raise ValueError("Failed to read any ZASCA cohort files")

    # combine all cohorts and process
    logger.debug("combining and processing cohort data")
    result = pd.concat(dfs, ignore_index=True)
    result = process_zasca_data(result)

    if save_processed:
        # ensure output directory exists
        output_dir = Path(DATA_DIR) / "processed"
        output_dir.mkdir(parents=True, exist_ok=True)

        # save processed data
        output_path = output_dir / "zasca_total.csv"
        logger.info("saving processed ZASCA data to %s", output_path)
        result.to_csv(output_path, index=False, encoding="utf-8-sig")

    logger.info("completed ZASCA data processing with %d records", len(result))
    return result


def read_processed_zasca() -> pd.DataFrame:
    """Read the pre-processed ZASCA data from CSV.

    Returns:
        pd.DataFrame: Processed ZASCA data from saved CSV file.
    """
    zasca_path = Path(DATA_DIR) / "processed/zasca_total.csv"
    logger.info("reading processed ZASCA data from %s", zasca_path)

    try:
        df = load_csv(zasca_path, encoding="utf-8-sig")

        logger.debug("successfully read %d ZASCA records", len(df))
        return df

    except Exception as e:
        logger.error("failed to read processed ZASCA data: %s", str(e))
        raise
