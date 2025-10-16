"""Sisbén group analysis functions for ZASCA vs national comparison."""

import logging
from pathlib import Path

import pandas as pd

from innpulsa.settings import DATA_DIR
from .utils import apply_sector_filter, MICRO_EMPRESA_THRESHOLD

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.sisben_groups")

SISBEN_MAPPING = {
    "A1-A5 (Pobreza extrema)": "A",
    "B1-B7 (Pobreza moderada)": "B",
    "C1-C18 (Vulnerabilidad)": "C",
    "D1-D21 (Ni pobre ni vulnerable)": "D",
}


@apply_sector_filter
def proporciones_grupos_sisben(df_zasca: pd.DataFrame, df_sisben: pd.DataFrame) -> pd.DataFrame:
    """Calculate Sisbén group proportions for ZASCA vs national average.

    Args:
        df_zasca: ZASCA data with sisben_emp1 column
        df_sisben: National Sisbén data with Grupo column

    Returns:
        pd.DataFrame: Prepared data for plotting with proportions by group

    """
    df_zasca = df_zasca.copy()
    df_sisben = df_sisben.copy()

    df_sisben["FEX"] = pd.to_numeric(df_sisben["FEX"], errors="coerce")

    # filter out companies with more than 10 employees for zasca
    df_zasca = df_zasca.loc[df_zasca["emp_total"] < MICRO_EMPRESA_THRESHOLD]

    df_zasca["grupo_clean"] = df_zasca["sisben_emp1"].replace(SISBEN_MAPPING)

    # only keep zasca obs with the sisben_mapping values
    df_zasca = df_zasca.loc[df_zasca["grupo_clean"].isin(list(SISBEN_MAPPING.values()))]

    # calculate zasca proportions
    zasca_dist = (
        df_zasca["grupo_clean"].value_counts(normalize=True).rename_axis("grupo").reset_index(name="percentage")
    )
    zasca_dist["source"] = "ZASCA"

    # calculate national sisben proportions (weighted by FEX)
    weighted_counts = df_sisben.groupby("Grupo", observed=True)["FEX"].sum()
    sisben_dist = (weighted_counts / weighted_counts.sum()).rename_axis("grupo").reset_index(name="percentage")
    sisben_dist["source"] = "SISBÉN Nacional"

    # calculate estimated "other support programme" composition
    # 43% D group, 57% for A-C groups combined
    other_support = pd.DataFrame({
        "grupo": ["Vulnerable", "No vulnerable"],
        "percentage": [0.43, 0.57],
        "source": "Otros programas de apoyo",
    })

    # combine all distributions
    final_df = pd.concat([zasca_dist, sisben_dist, other_support], ignore_index=True)

    # convert percentages to percentages
    final_df["percentage"] *= 100

    # sort by source and group for consistent ordering
    return final_df.sort_values(["source", "grupo"])


if __name__ == "__main__":
    SECTOR = "manufactura"
    df_zasca = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "zasca.csv", encoding="utf-8-sig")
    df_sisben = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "sisben.csv", encoding="utf-8-sig")

    logger.info("creating sisben group data for manufacturing sector")
    sisben_data = proporciones_grupos_sisben(df_zasca, df_sisben, filtro_por_sector=SECTOR)

    # save processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / SECTOR
    processed_dir.mkdir(parents=True, exist_ok=True)
    sisben_data.to_csv(processed_dir / "sisben_groups.csv", encoding="utf-8-sig", index=False)

    logger.info("saved sisben group data to %s", processed_dir / "sisben_groups.csv")
