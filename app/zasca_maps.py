"""Streamlit application to visualise ZASCA and RUES cohorts on maps.

Run with:
    streamlit run app/zasca_maps.py
"""

from __future__ import annotations

import unicodedata
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

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


def _load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load ZASCA & RUES CSVs, returning cleaned DataFrames."""
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

    # fillna in_rues column
    rues_total["in_rues"].fillna(False, inplace=True)

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
                    st.markdown("Contenido en preparación.")

    with strategies_tab:
        st.header("Estrategias")
        st.markdown(
            "Próximamente se detallarán las estrategias correspondientes a cada cohorte."
        )


if __name__ == "__main__":
    main()
