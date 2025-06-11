"""Streamlit application to visualise ZASCA and RUES cohorts on maps.

Run with:
    streamlit run app/zasca_maps.py
"""

from __future__ import annotations

import unicodedata
from pathlib import Path
from typing import List

import pandas as pd
import pydeck as pdk
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

DATA_DIR = Path("data/processed/geolocation")
ZASCA_ADDRESSES_PATH = DATA_DIR / "zasca_addresses.csv"
ZASCA_COORDS_PATH = DATA_DIR / "zasca_coordinates.csv"
RUES_COORDS_PATH = DATA_DIR / "rues_coordinates.csv"
ZASCA_TOTAL_PATH = DATA_DIR / "../zasca_total.csv"
RUES_TOTAL_PATH = DATA_DIR / "../rues_total.csv"

CITY_CONFIG = {
    "Cúcuta": {"center": (7.889, -72.505), "zoom": 12},
    "Medellín": {"center": (6.244, -75.574), "zoom": 12},
    "Bucaramanga": {"center": (7.119, -73.122), "zoom": 12},
}

# RGBA colours (R, G, B, A)
CLR_ZASCA_LIGHT = [50, 205, 50, 180]  # light-green  (not in RUES)
CLR_ZASCA_DARK = [0, 100, 0, 200]  # dark-green   (also in RUES)
CLR_RUES = [255, 0, 0, 160]  # red


def _normalise_str(s: str) -> str:
    """Return lowercase ASCII-only version of *s* (remove accents)."""
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        if isinstance(s, str)
        else ""
    )


@st.cache_data(show_spinner="Loading data...")
def _load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load ZASCA & RUES CSVs, returning cleaned DataFrames.

    Returns
    -------
    rues_total : DataFrame (con columna in_rues)
    zasca_coords : DataFrame
    rues_coords : DataFrame
    """
    zasca_addresses = pd.read_csv(ZASCA_ADDRESSES_PATH, encoding="utf-8-sig")
    zasca_coords = pd.read_csv(ZASCA_COORDS_PATH, encoding="utf-8-sig")
    rues_coords = pd.read_csv(RUES_COORDS_PATH, encoding="utf-8-sig")

    rues_total = pd.read_csv(RUES_TOTAL_PATH, encoding="utf-8-sig", low_memory=False)

    # filter rues_total to only include rows in rues_coords
    rues_total = rues_total[
        rues_total["nit"].isin(rues_coords["id"].astype(str))
        | rues_total["nit"].isin(zasca_addresses["nit"].astype(str))
    ].query("source_year == 2023")

    # merge in_rues column from zasca_addresses to rues_total
    rues_total = rues_total.merge(
        zasca_addresses[["nit", "in_rues"]],
        on="nit",
        how="left",
    )

    # [TO DO] Filter by ciiu principal

    # fillna in_rues column
    rues_total["in_rues"].fillna(False, inplace=True)

    # add in_rues to zasca_coords
    zasca_coords = zasca_coords.merge(
        zasca_addresses[["id", "in_rues"]],
        on="id",
        how="left",
    )

    # normalise column names
    zasca_coords.columns = [c.lower() for c in zasca_coords.columns]
    rues_coords.columns = [c.lower() for c in rues_coords.columns]

    # lat / lon may be strings; convert
    for df in (zasca_coords, rues_coords):
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df.dropna(subset=["latitude", "longitude"], inplace=True)

    return rues_total, zasca_coords, rues_coords


def _make_layer(df: pd.DataFrame, colour: list[int]) -> pdk.Layer:
    """Create a ScatterplotLayer from *df* with the given RGBA colour."""
    return pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position="[longitude, latitude]",
        get_fill_color=colour,
        get_radius=60,
        pickable=True,
    )


def main() -> None:
    st.set_page_config(page_title="Cohortes ZASCA y RUES", layout="wide")
    st.title("Cohortes ZASCA y RUES")

    rues_total, zasca_coords, rues_coords = _load_data()

    maps_tab, strategies_tab = st.tabs(["Mapas", "Estrategias"])

    with maps_tab:
        city_tabs = st.tabs(list(CITY_CONFIG.keys()))

        for city, tab in zip(CITY_CONFIG.keys(), city_tabs):
            with tab:
                cfg = CITY_CONFIG[city]

                # colour column for ZASCA based on match
                zasca_coords["colour"] = zasca_coords["in_rues"].map(
                    lambda m: CLR_ZASCA_DARK if m else CLR_ZASCA_LIGHT
                )

                layer_zasca = pdk.Layer(
                    "ScatterplotLayer",
                    data=zasca_coords,
                    get_position="[longitude, latitude]",
                    get_fill_color="colour",
                    get_radius=60,
                    pickable=True,
                )

                layer_rues = _make_layer(rues_coords, CLR_RUES)

                deck = pdk.Deck(
                    map_style="mapbox://styles/mapbox/light-v9",
                    initial_view_state=pdk.ViewState(
                        latitude=cfg["center"][0],
                        longitude=cfg["center"][1],
                        zoom=cfg["zoom"],
                        pitch=0,
                    ),
                    layers=[layer_rues, layer_zasca],  # RUES below, ZASCA on top
                    tooltip={
                        "html": (
                            "<div style='font-family: Arial, sans-serif; font-size: 12px;'>"
                            "<b>ID:</b> {id}<br/>"
                            "<b>Dirección:</b> {gmaps_address}</div>"
                        ),
                        "style": {
                            "backgroundColor": "#fefefe",
                            "color": "#333333",
                        },
                    },
                )

                # lay out: map (wide) | placeholder (narrow)
                col_map, col_side = st.columns([1, 1], gap="medium")

                with col_map:
                    st.subheader(city)
                    st.pydeck_chart(deck, use_container_width=True, height=800)

                with col_side:
                    st.subheader("Detalles")
                    st.markdown("Distribución de variables económicas.")

                    # densidades para la ciudad
                    city_norm = _normalise_str(city)
                    city_df = rues_total[
                        rues_total["city"].apply(_normalise_str).str.contains(city_norm)
                    ].copy()

                    if not city_df.empty:
                        fig = build_density_plot(
                            city_df,
                            [
                                "empleados",
                                "activos_total",
                                "cantidad_mujeres_empleadas",
                                "ingresos_actividad_ordinaria",
                            ],
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No hay datos de RUES para esta ciudad.")

    with strategies_tab:
        st.header("Estrategias")
        st.markdown(
            "Próximamente se detallarán las estrategias correspondientes a cada cohorte."
        )


_METRIC_LABELS = {
    "empleados": "Empleados",
    "activos_total": "Activos totales (COP)",
    "cantidad_mujeres_empleadas": "Mujeres empleadas",
    "ingresos_actividad_ordinaria": "Ingresos ordinarios (COP)",
}


def _to_log(series: pd.Series) -> pd.Series:
    """Convertir la serie a log10, omitiendo valores ≤ 0 o nulos."""
    num = pd.to_numeric(series, errors="coerce")
    num = num[num > 0]
    return np.log10(num)


def build_density_plot(df: pd.DataFrame, variables: List[str]) -> go.Figure:
    """Crear subplots con densidades para *variables* separadas por in_rues."""

    fig = make_subplots(
        rows=2, cols=2, subplot_titles=[_METRIC_LABELS[v] for v in variables]
    )

    pos = [(1, 1), (1, 2), (2, 1), (2, 2)]

    for var, (r, c) in zip(variables, pos):
        for flag, colour in zip([True, False], ["seagreen", "indianred"]):
            subset = df[df["in_rues"] == flag]
            values = _to_log(subset[var]).dropna()
            if values.empty:
                continue
            fig.add_trace(
                go.Histogram(
                    x=values,
                    histnorm="probability density",
                    name=f"{'ZASCA' if flag else 'Solo RUES'}",
                    marker_color=colour,
                    opacity=0.5,
                    showlegend=(r == 1 and c == 1),
                ),
                row=r,
                col=c,
            )

    # ejes en escala log (ya están los datos log10, pero evita confusión de bins)
    fig.update_xaxes(title_text="log₁₀(valor)", type="linear")
    fig.update_layout(height=500, margin=dict(t=40, r=10, l=10, b=10))
    return fig


if __name__ == "__main__":
    main()
