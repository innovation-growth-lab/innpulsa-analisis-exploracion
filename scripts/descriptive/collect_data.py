"""Load and run the data processing modules."""

import logging
from pathlib import Path
import pandas as pd

from innpulsa.loaders import load_zascas, load_rues
from innpulsa.processing.emicron import read_2024_emicron
from data_processing.utils import DEP_CODIGO, CIIU_MANUFACTURA
from innpulsa.settings import DATA_DIR

logger = logging.getLogger("innpulsa.scripts.descriptive.load_data")


def merge_2024_emicron() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load and merge 2024 EMICRON data, excluding 'Módulo de personal ocupado'.

    Returns:
        pd.DataFrame: Merged dataframe with all modules except personal ocupado

    """
    # load 2024 emicron data
    dfs_2024 = read_2024_emicron()

    # exclude 'Módulo de personal ocupado' from merging
    modules_to_merge = {k: v for k, v in dfs_2024.items() if k != "Módulo de personal ocupado"}

    logger.info("merging %d modules (excluding 'Módulo de personal ocupado')", len(modules_to_merge))

    # merge módulos
    emicron_2024 = next(iter(modules_to_merge.values()))
    for i, (module_name, df) in enumerate(modules_to_merge.items()):
        if i == 0:  # skip the first one
            continue

        # drop repeated variables from non-base datasets
        df_to_merge = df.drop(columns=["CLASE_TE", "COD_DEPTO", "AREA", "F_EXP"], errors="ignore")

        logger.debug("merging with %s", module_name)
        emicron_2024 = emicron_2024.merge(
            df_to_merge,
            on=["DIRECTORIO", "SECUENCIA_P", "SECUENCIA_ENCUESTA"],
            how="outer",
            suffixes=("", f"_{module_name}"),
        )
    logger.info("merged 2024 EMICRON data with %d records", len(emicron_2024))

    # left merge personal ocupado with merged_df, dropping repeated variables
    personal_ocupado = dfs_2024["Módulo de personal ocupado"]
    personal_ocupado = personal_ocupado.drop(columns=["CLASE_TE", "COD_DEPTO", "AREA", "F_EXP"], errors="ignore").merge(
        emicron_2024, on=["DIRECTORIO", "SECUENCIA_P", "SECUENCIA_ENCUESTA"], how="left"
    )

    return emicron_2024, personal_ocupado


def harmonise_zasca() -> pd.DataFrame:
    """Harmonise ZASCA data with EMICRON data.

    Returns:
        pd.DataFrame: Harmonised ZASCA data

    """
    df_zasca = load_zascas()

    # map departamento to codigo
    df_zasca["COD_DEPTO"] = df_zasca["dpto"].replace(DEP_CODIGO)

    # add manufacturing sector code to match EMICRON filtering
    df_zasca["GRUPOS12"] = 3  # manufacturing sector code

    return df_zasca


def load_sisben_data() -> pd.DataFrame:
    """Load and filter Sisbén data for relevant departments and columns.

    Returns:
        pd.DataFrame: Filtered Sisbén data with relevant columns only

    """
    # define relevant columns to load (reducing file size)
    relevant_columns: list[str] = [
        "cod_mpio",
        "Grupo",
        "FEX",
    ]

    # load sisben data with only relevant columns
    sisben_path = Path(DATA_DIR) / "innpulsa_raw" / "10_Insumos evaluación impacto" / "SISBEN" / "sisben.csv"
    df_sisben = pd.read_csv(sisben_path, usecols=relevant_columns, encoding="utf-8-sig", low_memory=False)  # type: ignore[reportArgumentType]

    # filter to relevant departments (first 2 digits of cod_mpio)
    # extract department codes from municipal codes
    df_sisben["COD_DEPTO"] = df_sisben["cod_mpio"].astype(str).str[:2].astype(int)

    # filter to departments that match our analysis
    df_sisben = df_sisben.loc[df_sisben["COD_DEPTO"].isin(DEP_CODIGO.values())]

    logger.info("loaded Sisbén data with %d records for relevant departments", len(df_sisben))

    return df_sisben


def load_rues_data() -> pd.DataFrame:
    """Load and filter RUES data for relevant departments and columns.

    Returns:
        pd.DataFrame: Filtered RUES data with relevant columns only

    """
    # load rues data
    df_rues = load_rues()

    # drop if no field21 (zipcode) or "" (empty string)
    df_rues = df_rues.dropna(subset=["field21"])
    df_rues = df_rues.loc[df_rues["field21"] != ""]

    # keep only if empleados =< 9
    df_rues = df_rues.loc[df_rues["empleados"] <= 9]  # noqa: PLR2004

    # sort by año_renovacion, and then remove duplicate matricula
    df_rues = df_rues.sort_values("año_renovacion")
    df_rues = df_rues.drop_duplicates(subset=["matricula"], keep="first")

    # extract department code from field21 (zipcode)
    df_rues["COD_DEPTO"] = df_rues["field21"].astype(str).str[:2].astype(int)

    # filter to relevant departments using DEP_CODIGO values
    relevant_dept_codes = list(DEP_CODIGO.values())
    df_rues = df_rues.loc[df_rues["COD_DEPTO"].isin(relevant_dept_codes)]

    # create GRUPOS12 being int 3 if two first characters of ciiu_principal are in CIIU_MANUFACTURA
    df_rues["GRUPOS12"] = df_rues["ciiu_principal"].astype(str).str[:2].isin(CIIU_MANUFACTURA).astype(int) * 3

    # keep only relevant columns
    relevant_columns = [
        "COD_DEPTO",
        "ingresos_actividad_ordinaria",
        "empleados",
        "cantidad_mujeres_empleadas",
        "source_year",
    ]
    df_rues = df_rues[relevant_columns]

    # remove duplicates, keeping latest year
    df_rues = df_rues.sort_values("source_year", ascending=False).drop_duplicates(
        subset=["COD_DEPTO", "ingresos_actividad_ordinaria", "empleados"], keep="first"
    )

    logger.info("loaded RUES data with %d records for relevant departments", len(df_rues))

    return df_rues


if __name__ == "__main__":
    output_dir = Path(DATA_DIR) / "01_raw" / "descriptive"
    output_dir.mkdir(parents=True, exist_ok=True)

    # load and merge 2024 emicron data
    df_2024_merged, df_personal_ocupado = merge_2024_emicron()

    # save to 01_raw_data/emicron_2024_merged.csv
    df_2024_merged.to_csv(output_dir / "emicron_2024_merged.csv", encoding="utf-8-sig", index=False)
    df_personal_ocupado.to_csv(output_dir / "personal_ocupado.csv", encoding="utf-8-sig", index=False)

    # load zasca data
    df_zasca = harmonise_zasca()

    # save to 01_raw_data/zasca.csv
    df_zasca.to_csv(output_dir / "zasca.csv", encoding="utf-8-sig", index=False)

    # load sisben data
    df_sisben = load_sisben_data()

    # save to 01_raw_data/sisben.csv
    df_sisben.to_csv(output_dir / "sisben.csv", encoding="utf-8-sig", index=False)

    # load rues data
    df_rues = load_rues_data()

    # save to 01_raw_data/rues.csv
    df_rues.to_csv(output_dir / "rues.csv", encoding="utf-8-sig", index=False)
