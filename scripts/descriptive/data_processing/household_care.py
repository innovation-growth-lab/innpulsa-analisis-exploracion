"""Process household care data from ZASCA."""

import logging
from pathlib import Path

import pandas as pd

from innpulsa.settings import DATA_DIR
from .utils import apply_sector_filter, MICRO_EMPRESA_THRESHOLD

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.household_care")


@apply_sector_filter
def household_care_data(df_zasca: pd.DataFrame) -> pd.DataFrame:
    """Extract household care data from ZASCA.

    Args:
        df_zasca: ZASCA data

    Returns:
        pd.DataFrame: Household care data with sector filtering applied

    """
    df_zasca = df_zasca.copy()

    # filter out companies with more than 10 employees for zasca
    df_zasca = df_zasca.loc[df_zasca["emp_total"] < MICRO_EMPRESA_THRESHOLD]

    # keep only the householdcare column
    result_df = df_zasca[["householdcare"]].copy()

    return result_df  # type: ignore[reportReturnStatementType]  # noqa: RET504


if __name__ == "__main__":
    SECTOR = "manufactura"
    df_zasca = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "zasca.csv", encoding="utf-8-sig")

    logger.info("creating household care data for manufacturing sector")
    household_care_data = household_care_data(df_zasca, filtro_por_sector=SECTOR)

    # save processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / SECTOR
    processed_dir.mkdir(parents=True, exist_ok=True)
    household_care_data.to_csv(processed_dir / "household_care.csv", encoding="utf-8-sig", index=False)

    logger.info("saved household care data to %s", processed_dir / "household_care.csv")
