"""
EMICRON data processing module.

This module handles the processing of EMICRON data, including reading,
cleaning, and aggregating data from multiple sources.
"""

import logging
from pathlib import Path
import pandas as pd
from ..settings import RAW_DATA_DIR


logger = logging.getLogger("innpulsa.processing.emicron")


def read_emicron() -> pd.DataFrame:
    """Read and process EMICRON data from multiple files.

    Returns:
        pd.DataFrame: Processed and aggregated EMICRON data with standardised columns.
    """
    emicron_dir = Path(RAW_DATA_DIR) / "EMICRON/2023"
    logger.info("reading EMICRON data from %s", emicron_dir)

    # define file paths
    files = {
        "characteristics": emicron_dir / "caracteristicas_23.dta",
        "costs": emicron_dir / "costos_gastos_activos_23.dta",
        "sales": emicron_dir / "sales_23.dta",
        "identification": emicron_dir / "identificacion_23.dta",
        "labor": emicron_dir / "labor_prop.dta",
    }

    # read base file
    logger.debug("reading base characteristics file")
    df = pd.read_stata(files["characteristics"])

    # merge with other files
    for name, file_path in files.items():
        if name == "characteristics":
            continue
        logger.debug("merging with %s data", name)
        temp_df = pd.read_stata(file_path)
        df = df.merge(
            temp_df,
            on=["DIRECTORIO", "SECUENCIA_P", "SECUENCIA_ENCUESTA"],
            how="left",
            suffixes=("", f"_{name}"),
        )

    # rename columns for standardisation
    column_mapping = {
        "P35": "sex_emp1",
        "P3032_1": "emp_total",
        "P3019": "capital",
        "VENTAS_MES_ANTERIOR": "sales2023q1s",
        "P1055": "estab_firm",
    }
    df = df.rename(columns=column_mapping)

    # filter data to relevant months
    logger.debug("filtering data to February-April period")
    df = df[df["MES_REF"].isin(["FEBRERO", "MARZO", "ABRIL"])]

    # create unique identifier
    df["numberid_emp1"] = (
        df["DIRECTORIO"].astype(str)
        + df["SECUENCIA_P"].astype(str)
        + df["SECUENCIA_ENCUESTA"].astype(str)
    )

    # define aggregation groups and variables
    group_cols = ["numberid_emp1", "AREA", "sex_emp1"]
    sum_vars = ["sales2023q1s", "VENTAS_MES_ANIO_ANTERIOR", "VENTAS_ANIO_ANTERIOR"]
    mean_vars = ["SUELDOS", "REMUNERACION_TOTAL", "capital", "emp_total"]
    weight_col = "F_EXP"

    # aggregate sum variables
    logger.debug("performing data aggregation")
    sum_df = df.groupby(group_cols)[sum_vars].sum()

    # calculate weighted means
    def weighted_mean(group):
        """Calculate weighted mean for specified columns."""
        weights = group[weight_col]
        return pd.Series(
            {col: (group[col] * weights).sum() / weights.sum() for col in mean_vars}
        )

    mean_df = df.groupby(group_cols).apply(weighted_mean)

    # combine aggregated data
    result = pd.concat([sum_df, mean_df], axis=1).reset_index()

    # convert all possible columns to numeric
    for col in result.columns:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    logger.info("completed EMICRON data processing with %d records", len(result))
    return result
