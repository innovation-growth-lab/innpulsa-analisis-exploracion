"""Analyze sales data across ZASCA, EMICRON, and RUES sources."""

import logging

import numpy as np
import pandas as pd

from data_processing.utils import apply_sector_filter

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.sales_analysis")


@apply_sector_filter
def sales(
    df_zasca: pd.DataFrame,
    df_emicron_2024_merged: pd.DataFrame,
    df_rues: pd.DataFrame,
    filtro_por_sector: str = "manufactura",
) -> pd.DataFrame:
    """Check sales data across ZASCA, EMICRON, and RUES sources.

    Args:
        df_zasca: ZASCA dataframe
        df_emicron_2024_merged: EMICRON 2024 merged dataframe
        df_rues: RUES dataframe
        filtro_por_sector: sector filter to apply

    Returns:
        pd.DataFrame: processed sales analysis data

    """
    logger.info("processing sales data for %s sector", filtro_por_sector)

    rng = np.random.default_rng(42)

    # process ZASCA sales data
    df_zasca_sales: pd.DataFrame = df_zasca[["sales2023"]].copy()  # type: ignore[assignmentTypeIgnore]
    df_zasca_sales = df_zasca_sales.dropna()
    df_zasca_sales["source"] = "ZASCA"
    df_zasca_sales["sales"] = df_zasca_sales["sales2023"]

    # process EMICRON sales data with weighted sampling
    df_emicron_sales = df_emicron_2024_merged[["VENTAS_ANIO_ANTERIOR", "F_EXP"]].copy()
    df_emicron_sales = df_emicron_sales.dropna()

    # weighted sampling for EMICRON
    weights = df_emicron_sales["F_EXP"] / df_emicron_sales["F_EXP"].sum()
    sample_indices = rng.choice(df_emicron_sales.index, size=5000, replace=True, p=weights)
    df_emicron_sample = df_emicron_sales.loc[sample_indices].copy()

    df_emicron_sample["source"] = "EMICRON"
    df_emicron_sample["sales"] = df_emicron_sample["VENTAS_ANIO_ANTERIOR"]

    # process RUES sales data
    df_rues_sales = df_rues[["ingresos_actividad_ordinaria"]].copy()
    df_rues_sales = df_rues_sales.dropna()

    # create representative sample for RUES
    sample_size = min(5000, len(df_rues_sales))
    sample_indices = rng.choice(df_rues_sales.index, size=sample_size, replace=False)
    df_rues_sample = df_rues_sales.loc[sample_indices].copy()

    df_rues_sample["source"] = "RUES"
    df_rues_sample["sales"] = df_rues_sample["ingresos_actividad_ordinaria"]

    # combine all sources
    sales_data = pd.concat(
        [
            df_zasca_sales[["source", "sales"]],
            df_emicron_sample[["source", "sales"]],
            df_rues_sample[["source", "sales"]],
        ],
        ignore_index=True,
    )

    # clean data - remove negative, zero, or very low sales (likely errors)
    sales_data = sales_data.loc[sales_data["sales"] >= 1000000]  # noqa: PLR2004

    # create sales categories for discretization
    def categorize_sales(sales):
        if sales < 10000000:  # < 10M
            return "1-10M"
        elif sales < 50000000:  # < 50M
            return "10-50M"
        elif sales < 200000000:  # < 200M
            return "50-200M"
        elif sales < 1000000000:  # < 1B
            return "200M-1B"
        else:
            return "1B+"

    sales_data["sales_category"] = sales_data["sales"].apply(categorize_sales)

    logger.info("processed sales data: %d observations", len(sales_data))

    return sales_data
