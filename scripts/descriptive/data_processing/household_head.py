"""Household head analysis functions for ZASCA data."""

import logging
from pathlib import Path

import pandas as pd

from innpulsa.settings import DATA_DIR
from .utils import apply_sector_filter, MICRO_EMPRESA_THRESHOLD

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.household_head")


@apply_sector_filter
def porcentaje_jefa_hogar(df_zasca: pd.DataFrame) -> pd.DataFrame:
    """Calculate percentage of participants who are household heads.

    Args:
        df_zasca: ZASCA data

    Returns:
        pd.DataFrame: Prepared data for plotting

    """
    df_zasca = df_zasca.copy()

    # filter out companies with more than 10 employees for zasca
    df_zasca = df_zasca.loc[df_zasca["emp_total"] < MICRO_EMPRESA_THRESHOLD]

    # calculate household head percentages
    household_dist = (
        df_zasca["headhousehold"]
        .value_counts(normalize=True)
        .rename_axis("household_head")
        .reset_index(name="percentage")
    )
    household_dist["source"] = "ZASCA"

    # convert percentages to percentages
    household_dist["percentage"] *= 100

    # sort by household head status
    return household_dist.sort_values("household_head")


if __name__ == "__main__":
    SECTOR = "manufactura"
    df_zasca = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "zasca.csv", encoding="utf-8-sig")

    logger.info("creating household head data for manufacturing sector")
    household_data = porcentaje_jefa_hogar(df_zasca, filtro_por_sector=SECTOR)

    # save processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / SECTOR
    processed_dir.mkdir(parents=True, exist_ok=True)
    household_data.to_csv(processed_dir / "household_head.csv", encoding="utf-8-sig", index=False)

    logger.info("saved household head data to %s", processed_dir / "household_head.csv")
