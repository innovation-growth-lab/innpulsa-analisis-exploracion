"""
ZASCA data processing module.

This module handles the processing of ZASCA data from multiple cohorts,
including reading, cleaning, and combining data.
"""

import logging
import pandas as pd

logger = logging.getLogger("innpulsa.processing.zasca")

MAPA_AFIRMATIVO = {"Sí": True, "No": False}
MAPA_SEXO = {"Masculino": False, "Femenino": True}
SALES_LIMITE = 10_000


def _merge_neighborhood(zasca_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge barrio and vereda columns into neighborhood column.

    Args:
        zasca_df: Raw ZASCA DataFrame

    Returns:
        Processed DataFrame with neighborhood column

    """
    if "barrio" in zasca_df.columns and "vereda" in zasca_df.columns:
        zasca_df["neighborhood"] = zasca_df["barrio"].fillna(zasca_df["vereda"])
        zasca_df = zasca_df.drop(columns=["barrio", "vereda"])
    return zasca_df


def _standardise_text_columns(zasca_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise text columns.

    Args:
        zasca_df: Raw ZASCA DataFrame

    Returns:
        Processed DataFrame with standardised text columns

    """
    if "estab_firm" in zasca_df.columns:
        zasca_df["estab_firm"] = zasca_df["estab_firm"].map(MAPA_AFIRMATIVO.get)
    if "sex_emp1" in zasca_df.columns:
        zasca_df["female"] = zasca_df["sex_emp1"].map(MAPA_SEXO.get)
    return zasca_df


def _convert_sales_to_millions(zasca_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert sales to millions.

    Args:
        zasca_df: Raw ZASCA DataFrame

    Returns:
        Processed DataFrame with sales converted to millions

    """
    for col in ["sales2023q1s", "sales2024q1s"]:
        if col in zasca_df.columns:
            mask = zasca_df[col] <= SALES_LIMITE
            zasca_df.loc[mask, col] *= 1_000_000
    return zasca_df


def _impute_sales(zasca_df: pd.DataFrame) -> pd.DataFrame:
    """
    Impute sales.

    Args:
        zasca_df: Raw ZASCA DataFrame

    Returns:
        Processed DataFrame with imputed sales

    """
    if "sales2023q1s" in zasca_df.columns and "weeklysales" in zasca_df.columns:
        mask = zasca_df["sales2023q1s"].isna()
        zasca_df.loc[mask, "sales2023q1s"] = zasca_df.loc[mask, "weeklysales"] * 4 * 3
    return zasca_df


def _remove_hyphen_from_nit(zasca_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove hyphen from NIT.

    Args:
        zasca_df: Raw ZASCA DataFrame

    Returns:
        Processed DataFrame with hyphen removed from NIT

    """
    zasca_df["nit"] = zasca_df["nit"].astype(str).str.replace(r"-\d+", "")
    return zasca_df


def _adjust_sales_for_bucaramanga(zasca_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adjust sales for Bucaramanga units (monthly to quarterly).

    Args:
        zasca_df: Raw ZASCA DataFrame

    Returns:
        Processed DataFrame with sales adjusted for Bucaramanga units

    """
    if "cohort" in zasca_df.columns:
        mask = zasca_df["cohort"] == "BMAC1"
        for col in ["sales2023q1s", "sales2024q1s"]:
            if col in zasca_df.columns:
                zasca_df.loc[mask, col] = zasca_df.loc[mask, col] / 11 * 3
    return zasca_df


def process_zasca(zasca_df: pd.DataFrame) -> pd.DataFrame:
    """Process raw ZASCA data by standardizing columns and values.

    Args:
        zasca_df: Raw ZASCA DataFrame

    Returns:
        Processed DataFrame with standardized columns and values

    """
    zasca_df = _merge_neighborhood(zasca_df)
    zasca_df["full_address"] = zasca_df["address"] + ", " + zasca_df["neighborhood"] + ", " + zasca_df["city"]
    zasca_df = _standardise_text_columns(zasca_df)

    # turn int to str for numberid_emp1 and nit
    if "numberid_emp1" in zasca_df.columns:
        zasca_df["numberid_emp1"] = zasca_df["numberid_emp1"].astype(pd.Int64Dtype()).astype(str)
    if "nit" in zasca_df.columns:
        zasca_df["nit"] = zasca_df["nit"].astype(str)

    zasca_df = _impute_sales(zasca_df)
    zasca_df = _adjust_sales_for_bucaramanga(zasca_df)
    zasca_df = _convert_sales_to_millions(zasca_df)
    return _remove_hyphen_from_nit(zasca_df)
