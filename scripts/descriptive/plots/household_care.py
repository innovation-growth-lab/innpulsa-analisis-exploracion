"""Create beeswarm plot for household care data."""

import logging
from pathlib import Path

import altair as alt
import pandas as pd
import numpy as np

from innpulsa.settings import DATA_DIR

logger = logging.getLogger("innpulsa.scripts.descriptive.plots.household_care")


def plot_household_care_violin(df_plot: pd.DataFrame) -> alt.LayerChart:
    """Create a violin plot for household care hours.

    Args:
        df_plot: the dataframe with householdcare column.

    Returns:
        an altair chart object.

    """
    # prepare data for violin plot
    df_plot = df_plot.copy()

    # cap values higher than 15 with random values between 12-14
    high_values_mask = df_plot["householdcare"] > 15  # noqa: PLR2004
    if high_values_mask.any():
        random_generator = np.random.Generator(np.random.PCG64())
        df_plot.loc[high_values_mask, "householdcare"] = random_generator.uniform(12, 14, high_values_mask.sum())

    # add jitter for dots positioning
    random_generator = np.random.Generator(np.random.PCG64())
    df_plot["jitter"] = random_generator.normal(0, 0.3, len(df_plot))

    # add more jitter to Y-axis values to avoid integer clustering
    y_jitter = random_generator.normal(0, 0.3, len(df_plot))
    df_plot["householdcare"] += y_jitter

    # create symmetric violin plot with dots
    density_data = (
        alt.Chart(df_plot)
        .transform_density("householdcare", as_=["value", "density"], extent=[0, 15], bandwidth=1.0)
        .transform_calculate(
            density_left="datum.density * -1",
            density_right="datum.density",
        )
        .transform_fold(["density_left", "density_right"], as_=["side", "density"])
        .data
    )

    # create the violin plot
    chart = (
        alt.Chart(density_data)
        .mark_area(orient="horizontal", opacity=0.8, color="#1F5DAD")
        .encode(
            x=alt.X("density:Q", axis=alt.Axis(labels=False, title=None, grid=False), scale=alt.Scale(domain=[-1, 1])),
            y=alt.Y(
                "value:Q",
                axis=alt.Axis(
                    title=None, labelFontSize=12, grid=False, values=[0, 5, 10, 15], labelExpr="datum.value + ' horas'"
                ),
                scale=alt.Scale(zero=False, domain=[0, 15]),
            ),
        )
        .properties(width=400, height=400)
    )

    # add dots for the actual data points
    dots = (
        alt.Chart(df_plot)
        .mark_circle(size=60, opacity=0.7, color="#1F5DAD")
        .encode(
            x=alt.X("jitter:Q", scale=alt.Scale(domain=[-1, 1])),
            y=alt.Y(
                "householdcare:Q",
                axis=alt.Axis(
                    title=None, labelFontSize=12, grid=False, values=[0, 5, 10, 15], labelExpr="datum.value + ' horas'"
                ),
                scale=alt.Scale(zero=False, domain=[0, 15]),
            ),
        )
    )

    # add mean line
    mean_value = df_plot["householdcare"].mean()
    mean_line = (
        alt.Chart(pd.DataFrame({"mean": [mean_value]}))
        .mark_rule(color="gray", strokeWidth=2, strokeDash=[5, 5])
        .encode(y=alt.Y("mean:Q"))
    )

    # combine violin, dots, and mean line
    chart += mean_line + dots

    return (
        chart.configure_view(strokeWidth=0)
        .configure_axis(domain=False, ticks=False, labels=False, grid=False)
        .configure_axisY(domain=True, ticks=True, labels=True, grid=False)
        .configure_axisX(domain=False, ticks=False, labels=False, grid=False)
    )


if __name__ == "__main__":
    # load processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / "manufactura"
    df_plot = pd.read_csv(processed_dir / "household_care.csv", encoding="utf-8-sig")

    # create and save plot
    chart = plot_household_care_violin(df_plot)

    # save as png
    output_dir = Path(DATA_DIR) / "03_outputs" / "descriptive" / "manufactura"
    output_dir.mkdir(parents=True, exist_ok=True)
    chart.save(str(output_dir / "household_care_beeswarm.png"), scale_factor=2.0, ppi=300)

    logger.info("saved household care beeswarm plot to %s", output_dir / "household_care_beeswarm.png")
