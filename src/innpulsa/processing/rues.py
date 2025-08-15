"""
RUES data processing module.

This module handles the processing of RUES (Registro Único Empresarial y Social) data,
including reading and combining data from multiple years.
"""

import logging
import pandas as pd

logger = logging.getLogger("innpulsa.processing.rues")

COLS_TO_KEEP = [
    "nit",
    "dirección_comercial",
    "ciiu_principal",
    "genero",
    "cantidad_mujeres_empleadas",
    "cantidad_mujeres_en_cargos_direc",
    "codigo_tamano_empresa",
    "cantidad_establecimientos",
    "activos_total",
    "empleados",
    "ingresos_actividad_ordinaria",
    "resultado_del_periodo",
    "source_year",
    "city",
    "state",
]


def process_rues(rues_df: pd.DataFrame, zip_df: pd.DataFrame) -> pd.DataFrame:
    """
    Read, (optionally) process, and optionally save the combined RUES data.

    Currently no additional processing is applied beyond combining years, but the
    function mirrors the ZASCA helper for consistency.

    Args:
        rues_df: DataFrame containing RUES data
        zip_df: DataFrame containing zipcode data

    Returns:
        DataFrame: Processed RUES data

    """
    zip_to_city: dict[str, str] = dict(zip(zip_df["province_code"].astype(str), zip_df["place"], strict=True))
    zip_to_state: dict[str, str] = dict(zip(zip_df["province_code"].astype(str), zip_df["state"], strict=True))

    # rename identifier column to 'nit' and ensure string type without decimals
    if "numero_de_identificacion" in rues_df.columns:
        rues_df = rues_df.rename(columns={"numero_de_identificacion": "nit"})

    if "nit" in rues_df.columns:
        # convert to string, drop trailing .0 that appears after converting from float
        rues_df["nit"] = rues_df["nit"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

        # remove empty nit rows
        rues_df = pd.DataFrame(rues_df[rues_df["nit"] != ""])

        # rename field15 and field21 to 'zipcode_comercial' and 'zipcode_fiscal'
        rues_df = rues_df.rename(columns={"field15": "zipcode_comercial", "field21": "zipcode_fiscal"})

    # add "Bogotá" in zip_to_city with zip being 11001
    zip_to_city["11001"] = "Bogotá"

    # zipcode-based city / region inference
    if "zipcode_comercial" in rues_df.columns and not zip_df.empty:
        rues_df["city"] = rues_df["zipcode_comercial"].map(zip_to_city.get)
        rues_df["state"] = rues_df["zipcode_comercial"].map(zip_to_state.get)

    rues_df = pd.DataFrame(rues_df[COLS_TO_KEEP])

    logger.info("completed RUES data processing with %d records", len(rues_df))
    return rues_df
