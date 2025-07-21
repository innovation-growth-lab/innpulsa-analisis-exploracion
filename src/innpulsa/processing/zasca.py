"""
ZASCA data processing module.

This module handles the processing of ZASCA data from multiple cohorts,
including reading, cleaning, and combining data.
"""

import logging
import pandas as pd

logger = logging.getLogger("innpulsa.processing.zasca")


def process_zasca(zasca_df: pd.DataFrame) -> pd.DataFrame:
    """Process raw ZASCA data by standardizing columns and values.

    Args:
        zasca_df: Raw ZASCA DataFrame

    Returns:
        Processed DataFrame with standardized columns and values
    """
    # merge barrio and vereda (only one is non-null)
    if "barrio" in zasca_df.columns and "vereda" in zasca_df.columns:
        zasca_df["neighborhood"] = zasca_df["barrio"].fillna(zasca_df["vereda"])
        zasca_df.drop(columns=["barrio", "vereda"], inplace=True)

    # create full_address column
    zasca_df["full_address"] = (
        zasca_df["address"] + ", " + zasca_df["neighborhood"] + ", " + zasca_df["city"]
    )

    # standardise text columns
    if "estab_firm" in zasca_df.columns:
        zasca_df["estab_firm"] = zasca_df["estab_firm"].map({"Sí": True, "No": False})

    if "sex_emp1" in zasca_df.columns:
        zasca_df["female"] = zasca_df["sex_emp1"].map(
            {"Masculino": False, "Femenino": True}
        )

    # turn int to str for numberid_emp1 and nit
    if "numberid_emp1" in zasca_df.columns:
        zasca_df["numberid_emp1"] = (
            zasca_df["numberid_emp1"].astype(pd.Int64Dtype()).astype(str)
        )
    if "nit" in zasca_df.columns:
        zasca_df["nit"] = zasca_df["nit"].astype(str)

    # adjust sales data for Bucaramanga units (monthly to quarterly)
    if "cohort" in zasca_df.columns:
        mask = zasca_df["cohort"] == "BMAC1"
        for col in ["sales2023q1s", "sales2024q1s"]:
            if col in zasca_df.columns:
                zasca_df.loc[mask, col] = zasca_df.loc[mask, col] / 11 * 3

    # impute missing sales2023q1s using weekly sales
    if "sales2023q1s" in zasca_df.columns and "weeklysales" in zasca_df.columns:
        mask = zasca_df["sales2023q1s"].isna()
        zasca_df.loc[mask, "sales2023q1s"] = zasca_df.loc[mask, "weeklysales"] * 4 * 3

    # convert sales from thousands to millions where needed
    for col in ["sales2023q1s", "sales2024q1s"]:
        if col in zasca_df.columns:
            mask = zasca_df[col] <= 10000
            zasca_df.loc[mask, col] *= 1_000_000

    # remove "-\d" from zasca NITs [VALIDATE]
    zasca_df["nit"] = zasca_df["nit"].astype(str).str.replace(r"-\d+", "")

    return zasca_df
