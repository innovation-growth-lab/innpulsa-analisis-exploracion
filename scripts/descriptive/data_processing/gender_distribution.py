"""Gender distribution analysis functions for comparing ZASCA and EMICRON data."""

import logging
from pathlib import Path

import pandas as pd

from innpulsa.settings import DATA_DIR
from .utils import apply_sector_filter, MICRO_EMPRESA_THRESHOLD, DEP_CODIGO

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.gender_distribution")


@apply_sector_filter
def diferencias_de_genero(df_zasca: pd.DataFrame, df_emicron_2024_merged: pd.DataFrame) -> pd.DataFrame:
    """Prepare ZASCA & EMICRON data for a stacked bar chart showing gender distribution.

    The output DataFrame is structured for a stacked bar chart in Altair,
    showing male and female proportions for both ZASCA and EMICRON.

    Args:
        df_zasca: ZASCA data
        df_emicron_2024_merged: EMICRON data

    Returns:
        pd.DataFrame: Prepared data for plotting

    """
    df_zasca = df_zasca.copy()
    df_emicron_2024_merged = df_emicron_2024_merged.copy()

    # apply same filtering as age distribution: drop companies with more than 10 employees for zasca
    df_zasca = df_zasca.loc[df_zasca["emp_total"] < MICRO_EMPRESA_THRESHOLD]

    # process zasca gender data
    # map zasca gender codes to standard format
    df_zasca["gender_clean"] = df_zasca["sex_emp1"].replace({1: "Masculino", 2: "Femenino"})
    zasca_gender_dist = (
        df_zasca["gender_clean"].value_counts(normalize=True).rename_axis("gender").reset_index(name="percentage")
    )
    zasca_gender_dist["source"] = "ZASCA"

    # process emicron gender data
    df_emicron_2024_merged = df_emicron_2024_merged.loc[
        df_emicron_2024_merged["COD_DEPTO"].isin(list(DEP_CODIGO.values()))
    ]

    # map emicron gender codes to standard format
    df_emicron_2024_merged["gender_clean"] = df_emicron_2024_merged["P3078"].replace({1: "Masculino", 2: "Femenino"})

    # calculate weighted gender distribution for emicron
    weighted_counts = df_emicron_2024_merged.groupby("gender_clean", observed=True)["F_EXP"].sum()
    emicron_gender_dist = (weighted_counts / weighted_counts.sum()).rename_axis("gender").reset_index(name="percentage")
    emicron_gender_dist["source"] = "EMICRON"

    # combine both distributions
    final_df = pd.concat([zasca_gender_dist, emicron_gender_dist], ignore_index=True)

    # convert percentages to percentages
    final_df["percentage"] *= 100

    # sort by source and gender for consistent ordering
    return final_df.sort_values(["source", "gender"])


if __name__ == "__main__":
    SECTOR = "manufactura"
    df_zasca = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "zasca.csv", encoding="utf-8-sig")
    df_emicron_2024_merged = pd.read_csv(
        Path(DATA_DIR) / "01_raw" / "descriptive" / "emicron_2024_merged.csv", encoding="utf-8-sig"
    )
    df_personal_ocupado = pd.read_csv(
        Path(DATA_DIR) / "01_raw" / "descriptive" / "personal_ocupado.csv", encoding="utf-8-sig"
    )

    logger.info("creating gender distribution data for manufacturing sector")
    gender_data = diferencias_de_genero(df_zasca, df_emicron_2024_merged, filtro_por_sector=SECTOR)

    # save processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / SECTOR
    processed_dir.mkdir(parents=True, exist_ok=True)
    gender_data.to_csv(processed_dir / "gender_distribution.csv", encoding="utf-8-sig", index=False)

    logger.info("saved gender distribution data to %s", processed_dir / "gender_distribution.csv")
