"""Age distribution analysis functions for comparing ZASCA and EMICRON data."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from innpulsa.settings import DATA_DIR
from .utils import apply_sector_filter, MICRO_EMPRESA_THRESHOLD

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.age_distribution")


@apply_sector_filter
def diferencias_de_edad(df_zasca: pd.DataFrame, df_emicron_2024_merged: pd.DataFrame) -> pd.DataFrame:
    """Prepare ZASCA & EMICRON data for a mirror histogram, including excess parts.

    The output DataFrame is structured for a stacked bar chart in Altair,
    with categories for 'common' and 'excess' distributions.

    Args:
        df_zasca: ZASCA data
        df_emicron_2024_merged: EMICRON data

    Returns:
        pd.DataFrame: Prepared data for plotting

    """
    df_zasca = df_zasca.copy()
    df_emicron_2024_merged = df_emicron_2024_merged.copy()

    bins = [18, 25, 35, 45, 55, 65, 75, 100]
    labels = [f"{bins[i]}-{bins[i + 1] - 1}" for i in range(len(bins) - 1)]

    # comparing to emicron, drop companies w more than 10 employees for zasca
    df_zasca = df_zasca.loc[df_zasca["emp_total"] < MICRO_EMPRESA_THRESHOLD]

    # create age_bins for ZASCA
    df_zasca["age"] = (
        pd.to_datetime("31/12/2024", format="%d/%m/%Y")
        - pd.to_datetime(df_zasca["birth_emp1"], errors="coerce", format="%d/%m/%Y")
    ).dt.days / 365.25
    df_zasca["age_bin"] = pd.cut(df_zasca["age"], bins=bins, labels=labels, right=False)
    zasca_dist = df_zasca["age_bin"].value_counts(normalize=True).rename_axis("age_bin").reset_index(name="percentage")

    # create age_bins for EMICRON
    df_emicron_2024_merged["age_bin"] = pd.cut(df_emicron_2024_merged["P241"], bins=bins, labels=labels, right=False)
    weighted_counts = df_emicron_2024_merged.groupby("age_bin", observed=True)["F_EXP"].sum()
    emicron_dist = (weighted_counts / weighted_counts.sum()).rename_axis("age_bin").reset_index(name="percentage")

    # merge ZASCA and EMICRON distributions
    comparison_df = zasca_dist.merge(emicron_dist, on="age_bin", suffixes=("_zasca", "_emicron"), how="outer")
    comparison_df = comparison_df.fillna({"percentage_zasca": 0, "percentage_emicron": 0})

    comparison_df["common"] = np.minimum(comparison_df["percentage_zasca"], comparison_df["percentage_emicron"])
    comparison_df["ZASCA_excess"] = comparison_df["percentage_zasca"] - comparison_df["common"]
    comparison_df["EMICRON_excess"] = comparison_df["percentage_emicron"] - comparison_df["common"]

    # reshape data into long format for Altair
    long_df = comparison_df.melt(
        id_vars=["age_bin"],
        value_vars=["common", "ZASCA_excess", "EMICRON_excess"],
        var_name="type",
        value_name="percentage",
    )

    # duplicate the 'common' rows so they exist for both sources
    common_rows = long_df[long_df["type"] == "common"].copy()
    common_zasca = common_rows.copy()
    common_zasca["source"] = "ZASCA"
    common_emicron = common_rows.copy()
    common_emicron["source"] = "EMICRON"

    # assign the correct source to the 'excess' rows
    excess_rows = long_df[long_df["type"] != "common"].copy()
    excess_rows["source"] = excess_rows["type"].str.replace("_excess", "")

    # combine all parts and filter out zero-value rows
    final_df = pd.concat([common_zasca, common_emicron, excess_rows]).query("percentage > 0")

    # create the value to plot (negative for ZASCA for the mirror effect)
    final_df["plot_value"] = np.where(
        final_df["source"] == "ZASCA", -final_df["percentage"] * 100, final_df["percentage"] * 100
    )

    # create a category for colouring the bars (e.g., 'ZASCA_common').
    final_df["color_category"] = final_df["source"] + "_" + final_df["type"].str.replace("_excess", "")

    return final_df.sort_values(["age_bin", "source"])


if __name__ == "__main__":
    SECTOR = "manufactura"
    df_zasca = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "zasca.csv", encoding="utf-8-sig")
    df_emicron_2024_merged = pd.read_csv(
        Path(DATA_DIR) / "01_raw" / "descriptive" / "emicron_2024_merged.csv", encoding="utf-8-sig"
    )
    df_personal_ocupado = pd.read_csv(
        Path(DATA_DIR) / "01_raw" / "descriptive" / "personal_ocupado.csv", encoding="utf-8-sig"
    )

    logger.info("creating age distribution data for manufacturing sector")
    age_data = diferencias_de_edad(df_zasca, df_emicron_2024_merged, filtro_por_sector=SECTOR)

    # save processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / SECTOR
    processed_dir.mkdir(parents=True, exist_ok=True)
    age_data.to_csv(processed_dir / "age_distribution.csv", encoding="utf-8-sig", index=False)

    logger.info("saved age distribution data to %s", processed_dir / "age_distribution.csv")
