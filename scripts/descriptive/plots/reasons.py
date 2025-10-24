"""Butterfly (mirror) plot for reasons-for-entrepreneurship (ZASCA vs EMICRON)."""

import logging

import altair as alt
import pandas as pd

logger = logging.getLogger("innpulsa.scripts.descriptive.plots.reasons")


def plot_reasons_butterfly(df_plot: pd.DataFrame) -> alt.LayerChart:
    """Create a butterfly chart with stacked common and excess bars.

    Args:
        df_plot: dataframe from data_processing.reasons with columns
                 ['category','type','value','source','plot_value','color_category'].

    Returns:
        alt.LayerChart

    """
    color_scale = alt.Scale(
        domain=["ZASCA_common", "ZASCA_ZASCA", "EMICRON_common", "EMICRON_EMICRON"],
        range=["#00B2A2", "#66D1C7", "#FF5836", "#FFAC9B"],
    )

    # prepare data with signed values for each part
    df_plot_signed = df_plot.copy()
    df_plot_signed["signed_value"] = df_plot_signed.apply(
        lambda r: -r["value"] * 100 if r["source"] == "ZASCA" else r["value"] * 100, axis=1
    )

    chart = (
        alt.Chart(df_plot_signed)
        .mark_bar()
        .encode(
            y=alt.Y(
                "category:N",
                axis=alt.Axis(labels=True, title="Motivos", grid=False, ticks=False, domain=False),
                sort=["Necesidad", "Oportunidad", "Tradición/Herencia", "Other"],
            ),
            x=alt.X(
                "signed_value:Q",
                axis=alt.Axis(labels=False, title=None, grid=False, ticks=False, domain=False),
            ),
            color=alt.Color("color_category:N", scale=color_scale, legend=None),
            order=alt.Order("type:N", sort="descending"),
        )
        .properties(
            width=875,
            height=520,
        )
    )

    # calculate total values for each source
    df_totals = df_plot.groupby(["category", "source"])["plot_value"].sum().reset_index()
    df_totals["total_value"] = df_totals["plot_value"]
    df_totals["abs_value"] = df_totals["total_value"].abs()
    df_totals["x_offset"] = df_totals["total_value"] + 5 * df_totals["total_value"].apply(lambda x: -1 if x < 0 else 1)
    df_totals["text_label"] = df_totals["abs_value"].round(0).astype(int).astype(str) + "%"

    # add text labels showing total percentages
    text_chart = (
        alt.Chart(df_totals)
        .mark_text(align="center", baseline="middle", fontSize=16, color="black")
        .encode(
            y=alt.Y("category:N", sort=["Necesidad", "Oportunidad", "Tradición/Herencia", "Other"]),
            x=alt.X("x_offset:Q"),
            text=alt.Text("text_label:N"),
        )
    )

    return (
        (chart + text_chart)
        .configure_view(strokeWidth=0)
        .configure_axis(domain=False, ticks=False, labels=False, grid=False)
        .configure_axisY(labels=True, title="Motivos", labelPadding=20, labelFontSize=14)
    )


def _plot_reasons_butterfly_base(df_plot: pd.DataFrame, hide_y_labels: bool = False) -> alt.LayerChart:
    """Create a butterfly chart without configuration for use in combined charts."""
    color_scale = alt.Scale(
        domain=["ZASCA_common", "ZASCA_ZASCA", "EMICRON_common", "EMICRON_EMICRON"],
        range=["#00B2A2", "#66D1C7", "#FF5836", "#FFAC9B"],
    )

    # prepare data with signed values for each part
    df_plot_signed = df_plot.copy()
    df_plot_signed["signed_value"] = df_plot_signed.apply(
        lambda r: -r["value"] * 100 if r["source"] == "ZASCA" else r["value"] * 100, axis=1
    )

    chart = (
        alt.Chart(df_plot_signed)
        .mark_bar()
        .encode(
            y=alt.Y(
                "category:N",
                axis=alt.Axis(
                    labels=not hide_y_labels, 
                    title="Motivos" if not hide_y_labels else "", 
                    grid=False, 
                    ticks=False, 
                    domain=False,
                    labelFontSize=14
                ),
                sort=["Necesidad", "Oportunidad", "Tradición/Herencia", "Other"],
            ),
            x=alt.X(
                "signed_value:Q",
                axis=alt.Axis(labels=False, title=None, grid=False, ticks=False, domain=False),
            ),
            color=alt.Color("color_category:N", scale=color_scale, legend=None),
            order=alt.Order("type:N", sort="descending"),
        )
        .properties(
            width=437,
            height=520,
        )
    )

    # calculate total values for each source
    df_totals = df_plot.groupby(["category", "source"])["plot_value"].sum().reset_index()
    df_totals["total_value"] = df_totals["plot_value"]
    df_totals["abs_value"] = df_totals["total_value"].abs()
    df_totals["x_offset"] = df_totals["total_value"] + 5 * df_totals["total_value"].apply(lambda x: -1 if x < 0 else 1)
    df_totals["text_label"] = df_totals["abs_value"].round(0).astype(int).astype(str) + "%"

    # add text labels showing total percentages
    text_chart = (
        alt.Chart(df_totals)
        .mark_text(align="center", baseline="middle", fontSize=16, color="black")
        .encode(
            y=alt.Y("category:N", sort=["Necesidad", "Oportunidad", "Tradición/Herencia", "Other"]),
            x=alt.X("x_offset:Q"),
            text=alt.Text("text_label:N"),
        )
    )

    return chart + text_chart


def plot_reasons_butterfly_combined(df_manu: pd.DataFrame, df_agro: pd.DataFrame) -> alt.HConcatChart:
    """Create side-by-side butterfly charts for manufacturing and agro sectors.

    Args:
        df_manu: manufacturing reasons data
        df_agro: agro reasons data

    Returns:
        alt.HConcatChart: Side-by-side butterfly plots

    """
    # Create agro chart (first) - with y-axis labels
    chart_agro = _plot_reasons_butterfly_base(df_agro, hide_y_labels=False)

    # Create manufacturing chart (second) - hide y-axis labels
    chart_manu = _plot_reasons_butterfly_base(df_manu, hide_y_labels=True)

    # Create the combined chart - agro first, manufactura second
    combined_chart = alt.hconcat(chart_agro, chart_manu, spacing=30)
    
    # Apply configuration to the combined chart
    return combined_chart.configure_view(strokeWidth=0).configure_axis(
        domain=False, ticks=False, labels=False, grid=False
    ).configure_axisY(
        labels=True, title="Motivos", labelPadding=20, labelFontSize=12
    )
