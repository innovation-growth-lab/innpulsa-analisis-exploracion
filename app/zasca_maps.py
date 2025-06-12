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
ZASCA_TOTAL_PATH = DATA_DIR / "../zasca_total.csv"
RUES_COORDS_PATH = DATA_DIR / "rues_coordinates.csv"
RUES_FILTERED_PATH = DATA_DIR / "rues_total_merged.csv"

CITY_CONFIG = {
    "Cúcuta": {"center": (7.889, -72.505), "zoom": 12},
    "Medellín": {"center": (6.244, -75.574), "zoom": 12},
    "Bucaramanga": {"center": (7.119, -73.122), "zoom": 12},
}

# RGBA colours (R, G, B, A)
CLR_ZASCA_LIGHT = [50, 205, 50, 180]  # light-green  (not in RUES)
CLR_ZASCA_DARK = [0, 100, 0, 200]  # dark-green   (also in RUES)
CLR_RUES = [255, 0, 0, 160]  # red

# Descripciones de actividades CIIU de interés
_CIIU_DESCRIPTIONS = {
    1521: "Fabricación de calzado de cuero y piel, con cualquier tipo de suela",
    1410: "Confección de prendas de vestir, excepto prendas de piel",
    4772: "Comercio al por menor de calzado y artículos de cuero y sucedáneos del cuero en establecimientos especializados",
    1522: "Fabricación de otros tipos de calzado, excepto de cuero y piel",
    1313: "Acabado de productos textiles",
    4642: "Comercio al por mayor de prendas de vestir",
}


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
    zasca_addresses = pd.read_csv(
        ZASCA_ADDRESSES_PATH, encoding="utf-8-sig", index_col=0
    )
    zasca_coords = pd.read_csv(ZASCA_COORDS_PATH, encoding="utf-8-sig", index_col=0)
    rues_coords = pd.read_csv(RUES_COORDS_PATH, encoding="utf-8-sig", index_col=0)
    rues_filtered = pd.read_csv(RUES_FILTERED_PATH, encoding="utf-8-sig", index_col=0)

    # enrich coordinate dataframe with economic activity and city info
    rues_coords = rues_coords.merge(
        rues_filtered,  # [["nit", "ciiu_principal", "city"]],
        left_on="id",
        right_on="nit",
        how="left",
    )

    # identify the top 3 ciiu_principal by major city among in_rues ==True
    top_3_ciiu_principal = (
        rues_filtered[
            (rues_filtered["in_rues"] == True)  # pylint: disable=C0121
            & (rues_filtered["city"].isin(["Medellin", "Cucuta", "Bucaramanga"]))
        ]
        .groupby(["ciiu_principal", "city"])
        .size()
        .reset_index(name="count")
        .sort_values(["city", "count"], ascending=[True, False])
        .groupby("city")
        .head(3)
    )

    # merge zasca nit to zasca_coords
    zasca_coords = zasca_coords.merge(
        zasca_addresses.reset_index()[["id", "nit"]],
        on="id",
        how="left",
    )

    # remove any "-\d" from zasca_coords nit
    zasca_coords["nit"] = (
        zasca_coords["nit"].str.replace(r"-\d+|\s\d+", "", regex=True)
    ).astype(pd.Int64Dtype())

    # merge rues_filtered
    zasca_coords = zasca_coords.merge(
        rues_filtered,
        on="nit",
        how="left",
    )

    # fill in_rues column from zasca_addresses, ciiu to int64
    zasca_coords["in_rues"] = (
        zasca_coords["in_rues"].fillna(False).infer_objects(copy=False)
    )
    zasca_coords["ciiu_principal"] = zasca_coords["ciiu_principal"].astype(
        pd.Int64Dtype()
    )

    # normalise column names
    zasca_coords.columns = [c.lower() for c in zasca_coords.columns]
    rues_coords.columns = [c.lower() for c in rues_coords.columns]

    # lat / lon may be strings; convert
    for df in (zasca_coords, rues_coords):
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df.dropna(subset=["latitude", "longitude"], inplace=True)

    return rues_filtered, top_3_ciiu_principal, zasca_coords, rues_coords


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
    """Main function to run the Streamlit app."""
    st.set_page_config(page_title="Cohortes ZASCA y RUES", layout="wide")
    st.title("Cohortes ZASCA y RUES")

    rues_filtered, top_3_ciiu_principal, zasca_coords, rues_coords = _load_data()

    maps_tab, strategies_tab = st.tabs(["Mapas", "Estrategias"])

    with maps_tab:
        city_tabs = st.tabs(list(CITY_CONFIG.keys()))

        for city, tab in zip(CITY_CONFIG.keys(), city_tabs):
            with tab:
                cfg = CITY_CONFIG[city]

                # Selector para filtrar por CIIU principal (top-3 de la ciudad)
                city_norm = _normalise_str(city)
                ciiu_opts = top_3_ciiu_principal[
                    top_3_ciiu_principal["city"].apply(_normalise_str) == city_norm
                ]["ciiu_principal"].tolist()

                # crear dos columnas para no ocupar todo el ancho
                col_filter, _, col_red = st.columns([1, 3, 1])
                with col_filter:
                    # asegurar que los códigos son enteros para mapear descripción
                    ciiu_opts_int = [int(x) for x in ciiu_opts]
                    labels_top3 = [_format_ciiu(c) for c in ciiu_opts_int]

                    select_options = ["Todas"]
                    if labels_top3:
                        select_options.append("Top 3")
                    select_options += labels_top3

                    selected_label = st.selectbox(
                        "Actividad económica (CIIU)",
                        select_options,
                        index=0,
                        key=f"ciiu_sel_{city}",
                    )

                with col_red:
                    # checkbox para ocultar/mostrar los puntos Solo RUES
                    show_rues = st.checkbox(
                        "Mostrar puntos Solo RUES (rojo)",
                        value=True,
                        key=f"show_rues_{city}",
                    )

                # determinar los códigos seleccionados para filtrar
                if selected_label == "Todas":
                    codes_filter: list[int] | None = None
                elif selected_label == "Top 3":
                    codes_filter = ciiu_opts_int
                else:
                    label_to_code = {_format_ciiu(c): c for c in ciiu_opts_int}
                    codes_filter = [label_to_code[selected_label]]

                # Preparar dataframes filtrados para el mapa
                zasca_plot_df = zasca_coords.copy()
                rues_plot_df = rues_coords.copy()

                # # filtrar por ciudad
                # zasca_plot_df = zasca_plot_df[
                #     zasca_plot_df["city"].apply(_normalise_str) == city_norm
                # ]
                # rues_plot_df = rues_plot_df[
                #     rues_plot_df["city"].apply(_normalise_str) == city_norm
                # ]

                # filtrar por actividad económica (si procede)
                if codes_filter is not None:
                    zasca_plot_df = zasca_plot_df[
                        zasca_plot_df["ciiu_principal"].isin(codes_filter)
                    ]
                    rues_plot_df = rues_plot_df[
                        rues_plot_df["ciiu_principal"].isin(codes_filter)
                    ]

                # colour column for ZASCA based on match
                zasca_plot_df["colour"] = zasca_plot_df["in_rues"].map(
                    lambda m: CLR_ZASCA_DARK if m else CLR_ZASCA_LIGHT
                )

                layer_zasca = pdk.Layer(
                    "ScatterplotLayer",
                    data=zasca_plot_df,
                    get_position="[longitude, latitude]",
                    get_fill_color="colour",
                    get_radius=60,
                    pickable=True,
                )

                layer_rues = _make_layer(rues_plot_df, CLR_RUES)

                deck = pdk.Deck(
                    map_style="mapbox://styles/mapbox/light-v9",
                    initial_view_state=pdk.ViewState(
                        latitude=cfg["center"][0],
                        longitude=cfg["center"][1],
                        zoom=cfg["zoom"],
                        pitch=0,
                    ),
                    layers=([layer_rues] if show_rues else []) + [layer_zasca],
                    tooltip={
                        "html": (
                            "<div style='font-family: Arial, sans-serif; font-size: 12px;'>"
                            "<b>NIT:</b> {nit}<br/>"
                            "<b>CIIU:</b> {ciiu_principal}<br/>"
                            "<b>Empleados:</b> {empleados}<br/>"
                            "<b>Activos totales:</b> {activos_total}<br/>"
                            "<b>Mujeres empleadas:</b> {cantidad_mujeres_empleadas}<br/>"
                            "<b>Ingresos ordinarios:</b> {ingresos_actividad_ordinaria}<br/>"
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
                    city_df = rues_filtered[
                        rues_filtered["city"]
                        .apply(_normalise_str)
                        .str.contains(city_norm)
                    ].copy()

                    if codes_filter is not None:
                        city_df = city_df[city_df["ciiu_principal"].isin(codes_filter)]

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
                    name=f"{'ZASCA+RUES' if flag else 'Solo RUES'}",
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


def _format_ciiu(ciiu_code: int) -> str:  # helper para mostrar "1521 – descripción"
    description = _CIIU_DESCRIPTIONS.get(ciiu_code, "")
    return f"{ciiu_code} – {description}" if description else str(ciiu_code)


if __name__ == "__main__":
    main()
