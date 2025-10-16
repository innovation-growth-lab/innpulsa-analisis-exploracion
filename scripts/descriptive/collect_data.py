"""Load and run the data processing modules."""

import logging
from pathlib import Path
import pandas as pd

from innpulsa.loaders import load_zascas
from innpulsa.processing.emicron import read_2024_emicron
from innpulsa.settings import DATA_DIR

logger = logging.getLogger("innpulsa.scripts.descriptive.load_data")


DEP_CODIGO = {
    "ANTIOQUIA": 5,
    "ATLÁNTICO": 8,
    "BOGOTÁ, D.C.": 11,
    "BOLÍVAR": 13,
    "CALDAS": 17,
    "CUNDINAMARCA": 25,
    "LA GUAJIRA": 44,
    "NORTE DE SANTANDER": 54,
    "SANTANDER": 68,
    "VALLE DEL CAUCA": 76,
}


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
