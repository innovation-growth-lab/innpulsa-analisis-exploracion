"""
RUES data processing module.

This module handles the processing of RUES (Registro Único Empresarial y Social) data,
including reading and combining data from multiple years.
"""

from pathlib import Path
import logging
import pandas as pd
from ..settings import RAW_DATA_DIR, DATA_DIR
from ..loaders import load_zipcodes_co, load_stata, load_csv

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


def read_rues() -> pd.DataFrame:
    """Read and combine RUES data from multiple years (raw)."""

    rues_dir = Path(RAW_DATA_DIR) / "Rues"

    files = {
        2023: rues_dir / "Activas y renovadas 2023-marz2024.dta",
        2024: rues_dir / "Activas y renovadas 2024-marz2025.dta",
    }

    dfs = []
    for year, file_path in files.items():
        try:
            logger.debug("reading RUES file: %s", file_path)
            df = load_stata(file_path, pyreadstat=False)
            df["source_year"] = year
            dfs.append(df)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("failed to read %s: %s", file_path, exc)

    if not dfs:
        raise ValueError("No RUES files could be read.")

    return pd.concat(dfs, ignore_index=True)


def read_and_process_rues(save_processed: bool = True) -> pd.DataFrame:
    """Read, (optionally) process, and optionally save the combined RUES data.

    Currently no additional processing is applied beyond combining years, but the
    function mirrors the ZASCA helper for consistency.
    """

    logger.info("reading and combining RUES datasets")
    df = read_rues()

    logger.info("load zipcode lookup")
    zip_raw = load_zipcodes_co()
    zip_df = pd.DataFrame(zip_raw)
    logger.info("loaded %d zipcodes", len(zip_df))

    zip_to_city = dict(zip(zip_df["province_code"].astype(str), zip_df["place"]))
    zip_to_state = dict(zip(zip_df["province_code"].astype(str), zip_df["state"]))

    # rename identifier column to 'nit' and ensure string type without decimals
    if "numero_de_identificacion" in df.columns:
        df = df.rename(columns={"numero_de_identificacion": "nit"})

    if "nit" in df.columns:
        # convert to string, drop trailing .0 that appears after converting from float
        df["nit"] = (
            df["nit"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        )

        # remove empty nit rows
        df = df[df["nit"] != ""]

        # rename field15 and field21 to 'zipcode_comercial' and 'zipcode_fiscal'
        df = df.rename(
            columns={"field15": "zipcode_comercial", "field21": "zipcode_fiscal"}
        )

    # zipcode-based city / region inference
    if "zipcode_comercial" in df.columns and not zip_df.empty:
        df["city"] = df["zipcode_comercial"].map(zip_to_city)
        df["state"] = df["zipcode_comercial"].map(zip_to_state)

    df = df[COLS_TO_KEEP]

    if save_processed:
        output_dir = Path(DATA_DIR) / "processed"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "rues_total.csv"
        logger.info("saving processed RUES data to %s", output_path)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

    logger.info("completed RUES data processing with %d records", len(df))
    return df


def read_processed_rues() -> pd.DataFrame:
    """Load the saved combined RUES CSV."""

    path = Path(DATA_DIR) / "processed/rues_total.csv"
    logger.info("reading processed RUES data from %s", path)
    return load_csv(path, encoding="utf-8-sig", low_memory=False)
