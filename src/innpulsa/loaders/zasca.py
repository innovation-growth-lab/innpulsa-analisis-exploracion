"""
ZASCA data loading module.

This module handles the loading of ZASCA data from multiple cohorts
"""

import logging
from pathlib import Path
import pandas as pd
from innpulsa.settings import RAW_DATA_DIR, DATA_DIR
from .generic import load_csv, load_stata

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
    "employees_w",
    "emp_internc",
    "reason2start",
    "rut",
    "bookkeeping",
    "hascredit",
    "GENERO",
    "TAMANIO_EMPRESA",
    "DEPARTAMENTO",
]

# rename wrongly spelled departamento values
DPTO_CORRECTED = {
    "BOLÃVAR": "BOLÍVAR",
    "BOGOTÃ, D.C.": "BOGOTÁ, D.C.",
    "BOGOTÁ. D.C.": "BOGOTÁ, D.C.",
    "ATLÃNTICO": "ATLÁNTICO",
    "NARIÃO": "NARIÑO",
}

logger = logging.getLogger("innpulsa.loaders.zasca")


def load_zascas() -> pd.DataFrame:
    """Read and combine the ZASCA data from multiple sources and sectors.

    Combines data from:
    - Zascas_cerrados.csv: Closed ZASCA cohorts (manufacturing)
    - zascas_manufactura_anonima.csv: Manufacturing ZASCA cohorts
    - agro_anonimizado.dta: Agriculture ZASCA cohorts

    Adds GRUPOS12 column to identify sectors:
    - 3: Manufacturing sector
    - 1: Agriculture sector

    Returns:
        pd.DataFrame: Combined ZASCA data from all sources with sector identification.

    """
    # Load closed zascas (manufacturing)
    logger.info("loading closed ZASCA data from Zascas_cerrados.csv")
    closed_zascas = pd.read_csv(Path(RAW_DATA_DIR) / "Zascas_cerrados.csv", encoding="utf-8-sig", low_memory=False)
    closed_zascas = select_relevant_columns(closed_zascas, closed_zascas.columns.tolist())
    closed_zascas["cohort"] = closed_zascas["cohort"].astype(str) + closed_zascas["centro"].astype(str)
    closed_zascas["GRUPOS12"] = 3  # manufacturing sector
    logger.info("loaded %d closed ZASCA records (manufacturing)", len(closed_zascas))

    # Load manufacturing zascas
    logger.info("loading manufacturing ZASCA data from zascas_manufactura_anonima.csv")
    manufacturing_zascas = pd.read_csv(
        Path(RAW_DATA_DIR) / "zascas_manufactura_anonima.csv", encoding="utf-8-sig", low_memory=False
    )
    manufacturing_zascas = select_relevant_columns(manufacturing_zascas, manufacturing_zascas.columns.tolist())
    manufacturing_zascas["cohort"] = manufacturing_zascas["cohort"].astype(str) + manufacturing_zascas["centro"].astype(
        str
    )
    manufacturing_zascas["dpto"] = manufacturing_zascas["dpto"].replace(DPTO_CORRECTED)
    manufacturing_zascas["GRUPOS12"] = 3  # manufacturing sector
    logger.info("loaded %d manufacturing ZASCA records", len(manufacturing_zascas))

    # Load agro zascas
    logger.info("loading agro ZASCA data from agro_anonimizado.dta")
    agro_zascas = load_stata(Path(RAW_DATA_DIR) / "agro_anonimizado.dta")
    agro_zascas = select_relevant_columns(agro_zascas, agro_zascas.columns.tolist())
    if "cohort" in agro_zascas.columns:
        agro_zascas["cohort"] = agro_zascas["cohort"].astype(str) + agro_zascas["centro"].astype(str)
    # rename columns {GENERO, TAMANIO_EMPRESA, DEPARTAMENTO} to {sex_emp1, size_emp, dpto}
    agro_zascas = agro_zascas.rename(
        columns={"GENERO": "sex_emp1", "TAMANIO_EMPRESA": "size_emp", "DEPARTAMENTO": "dpto"}
    )
    agro_zascas["size_emp"] = agro_zascas["size_emp"].apply(
        lambda x: 9 if str(x).strip().lower() == "microempresa" else 10
    )
    agro_zascas["sex_emp1"] = agro_zascas["sex_emp1"].replace({"Hombre": "Masculino", "Mujer": "Femenino"})
    agro_zascas["dpto"] = agro_zascas["dpto"].replace(DPTO_CORRECTED)
    agro_zascas["GRUPOS12"] = 1  # agriculture sector
    logger.info("loaded %d agro ZASCA records", len(agro_zascas))

    # Combine all datasets
    logger.info("combining ZASCA datasets")
    combined_zascas = pd.concat([closed_zascas, manufacturing_zascas, agro_zascas], ignore_index=True)
    logger.info(
        "combined total: %d ZASCA records (manufacturing: %d, agro: %d)",
        len(combined_zascas),
        len(closed_zascas) + len(manufacturing_zascas),
        len(agro_zascas),
    )

    return combined_zascas


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
