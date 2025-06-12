from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from constants import CIIU_DESCRIPTIONS

# Metric labels (shared across plots)
METRIC_LABELS = {
    "empleados": "Empleados",
    "activos_total": "Activos totales (COP)",
    "cantidad_mujeres_empleadas": "Mujeres empleadas",
    "ingresos_actividad_ordinaria": "Ingresos ordinarios (COP)",
    "sales2022s": "Ventas 2022 (COP)",
    "emp_total": "Empleo total (ZASCA)",
}


# Helpers
def to_log(series: pd.Series) -> pd.Series:
    """Convert the series to log10, omitting values ≤ 0 or null."""
    num = pd.to_numeric(series, errors="coerce")
    num = num[num > 0]
    return np.log10(num)


def format_ciiu(ciiu_code: int) -> str:
    """Return label 'code – description' when available."""
    description = CIIU_DESCRIPTIONS.get(int(ciiu_code), "")
    return f"{ciiu_code} – {description}" if description else str(ciiu_code)


# Density plots
def build_density_plot(df: pd.DataFrame, variables: List[str]) -> go.Figure:
    """Create density subplots for *variables* separated by in_rues (True/False)."""

    # decide layout
    rows, cols = (1, 2) if len(variables) == 2 else (2, 2)

    fig = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=[METRIC_LABELS.get(v, v) for v in variables],
    )

    colours = ["seagreen", "indianred"]
    legend_names = ["ZASCA+RUES", "Solo RUES"]

    for i, var in enumerate(variables):
        r = i // cols + 1
        c = i % cols + 1
        for flag, colour, legend in zip([True, False], colours, legend_names):
            values = to_log(df.loc[df["in_rues"] == flag, var]).dropna()
            if values.empty:
                continue
            fig.add_trace(
                go.Histogram(
                    x=values,
                    histnorm="probability density",
                    name=legend,
                    marker_color=colour,
                    opacity=0.5,
                    showlegend=(i == 0),
                ),
                row=r,
                col=c,
            )

    fig.update_xaxes(title_text="log₁₀(valor)", type="linear")
    fig.update_layout(height=400, margin=dict(t=40, r=10, l=10, b=10))
    return fig


def build_density_plot_zasca(df: pd.DataFrame, variables: List[str]) -> go.Figure:
    """Density plots for ZASCA companies: in_rues True vs False (greens)."""

    rows, cols = (1, 2) if len(variables) == 2 else (2, 2)

    fig = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=[METRIC_LABELS.get(v, v) for v in variables],
    )

    colours = ["darkgreen", "lightgreen"]
    legend_names = ["En RUES", "Sin RUES"]

    for i, var in enumerate(variables):
        r = i // cols + 1
        c = i % cols + 1
        for flag, colour, legend in zip([True, False], colours, legend_names):
            values = to_log(df.loc[df["in_rues"] == flag, var]).dropna()
            if values.empty:
                continue
            fig.add_trace(
                go.Histogram(
                    x=values,
                    histnorm="probability density",
                    name=legend,
                    marker_color=colour,
                    opacity=0.5,
                    showlegend=(i == 0),
                ),
                row=r,
                col=c,
            )

    fig.update_xaxes(title_text="log₁₀(valor)", type="linear")
    fig.update_layout(height=300, margin=dict(t=40, r=10, l=10, b=10))
    return fig
