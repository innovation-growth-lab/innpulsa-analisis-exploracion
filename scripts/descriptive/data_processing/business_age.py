"""Business age analysis functions for comparing ZASCA and EMICRON data."""

import logging
from pathlib import Path
from datetime import date

import numpy as np
import pandas as pd

from innpulsa.settings import DATA_DIR
from .utils import apply_sector_filter, MICRO_EMPRESA_THRESHOLD, DEP_CODIGO

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.business_age")

rng = np.random.default_rng(42)

# mapping for P639 categorical
EMICRON_AGE_MAPPING = {
    1: (0, 1),
    2: (1, 3),
    3: (3, 5),
    4: (5, 10),
    5: (10, 50),
}


@apply_sector_filter
def business_age_analysis(df_zasca: pd.DataFrame, df_emicron_2024_merged: pd.DataFrame) -> pd.DataFrame:
    """Analyse business age distribution comparing ZASCA and EMICRON data.

    Args:
        df_zasca: ZASCA data with yearsales column
        df_emicron_2024_merged: EMICRON data with P639 column

    Returns:
        pd.DataFrame: Combined data with precise business age information for both sources

    """
    df_zasca = df_zasca.copy()
    df_emicron_2024_merged = df_emicron_2024_merged.copy()

    # filter ZASCA data (companies with less than 10 employees)
    df_zasca = df_zasca.loc[df_zasca["emp_total"] < MICRO_EMPRESA_THRESHOLD]

    # process ZASCA data - create random dates within each year
    df_zasca = df_zasca.dropna(subset=["yearsales"])
    df_zasca["yearsales"] = pd.to_numeric(df_zasca["yearsales"], errors="coerce")
    df_zasca = df_zasca.dropna(subset=["yearsales"])

    # create random dates within each year for ZASCA
    def create_random_date_in_year(year):
        # create random month (1-12) and day (1-28 to avoid month-end issues)
        month = rng.integers(1, 13)
        day = rng.integers(1, 29)
        return date(year, month, day)

    df_zasca["start_date"] = df_zasca["yearsales"].apply(create_random_date_in_year)

    # calculate precise business age in years
    current_date = date(2024, 12, 31)  # end of 2024
    df_zasca["business_age"] = df_zasca["start_date"].apply(lambda x: (current_date - x).days / 365.25)

    # add source identifier
    df_zasca["source"] = "ZASCA"

    # process EMICRON data
    df_emicron_2024_merged = df_emicron_2024_merged.loc[
        df_emicron_2024_merged["COD_DEPTO"].isin(list(DEP_CODIGO.values()))
    ]

    # filter out missing P639 values
    df_emicron_2024_merged = df_emicron_2024_merged.dropna(subset=["P639"])
    df_emicron_2024_merged["P639"] = pd.to_numeric(df_emicron_2024_merged["P639"], errors="coerce")
    df_emicron_2024_merged = df_emicron_2024_merged.dropna(subset=["P639"])

    # calculate weights (normalized expansion factors)
    weights = df_emicron_2024_merged["F_EXP"] / df_emicron_2024_merged["F_EXP"].sum()

    # sample 5000 observations with replacement using weights
    sample_indices = rng.choice(df_emicron_2024_merged.index, size=5000, replace=True, p=weights)
    df_emicron_sample = df_emicron_2024_merged.loc[sample_indices].copy()

    # convert P639 categorical to age ranges and create random dates within those ranges
    def create_random_date_in_age_range(age_range):
        min_age, max_age = age_range
        end_date = current_date
        start_date_max = date(end_date.year - int(min_age), end_date.month, end_date.day)
        start_date_min = date(end_date.year - int(max_age), end_date.month, end_date.day)

        # create random date within the range
        random_factor = rng.random()
        return start_date_min + (start_date_max - start_date_min) * random_factor

    df_emicron_sample["age_range"] = df_emicron_sample["P639"].map(EMICRON_AGE_MAPPING)
    df_emicron_sample["start_date"] = df_emicron_sample["age_range"].apply(create_random_date_in_age_range)

    # calculate precise business age in years
    df_emicron_sample["business_age"] = df_emicron_sample["start_date"].apply(
        lambda x: (current_date - x).days / 365.25 if pd.notna(x) else np.nan
    )
    df_emicron_sample = df_emicron_sample.dropna(subset=["business_age"])

    # add source identifier
    df_emicron_sample["source"] = "EMICRON"

    # select relevant columns for both datasets
    zasca_cols = ["business_age", "start_date", "source"]
    emicron_cols = ["business_age", "start_date", "source"]

    # combine datasets
    result: pd.DataFrame = pd.concat([df_zasca[zasca_cols], df_emicron_sample[emicron_cols]], ignore_index=True)  # type: ignore[assignment]
    return result


if __name__ == "__main__":
    SECTOR = "manufactura"
    df_zasca = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "zasca.csv", encoding="utf-8-sig")
    df_emicron_2024_merged = pd.read_csv(
        Path(DATA_DIR) / "01_raw" / "descriptive" / "emicron_2024_merged.csv", encoding="utf-8-sig"
    )

    logger.info("creating business age analysis data for manufacturing sector")
    business_age_data = business_age_analysis(df_zasca, df_emicron_2024_merged, filtro_por_sector=SECTOR)

    # save processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / SECTOR
    processed_dir.mkdir(parents=True, exist_ok=True)
    business_age_data.to_csv(processed_dir / "business_age.csv", encoding="utf-8-sig", index=False)

    logger.info("saved business age data to %s", processed_dir / "business_age.csv")
