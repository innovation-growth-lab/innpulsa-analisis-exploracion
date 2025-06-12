"""Streamlit entrypoint for ZASCA & RUES visualisation.

Run with:
    streamlit run app/app.py
"""

from __future__ import annotations

import streamlit as st
import pydeck as pdk

from constants import CITY_CONFIG, CLR_ZASCA_DARK, CLR_ZASCA_LIGHT, CLR_RUES
from data_loader import load_data, _normalise_str
from plots import (
    build_density_plot,
    build_density_plot_zasca,
    format_ciiu,
)
from map_components import make_layer


@st.cache_data(show_spinner="Cargando datos...")
def get_data():
    """Get data from the data loader."""
    return load_data()


def main() -> None:
    """Main function to run the app."""
    st.set_page_config(page_title="Cohortes ZASCA y RUES", layout="wide")
    st.title("Cohortes ZASCA y RUES")

    (
        rues_filtered,
        top_3_ciiu_principal,
        zasca_coords,
        rues_coords,
    ) = get_data()

    maps_tab, strategies_tab = st.tabs(["Mapas", "Estrategias"])

    with maps_tab:
        city_tabs = st.tabs(list(CITY_CONFIG.keys()))

        for city, tab in zip(CITY_CONFIG.keys(), city_tabs):
            with tab:
                cfg = CITY_CONFIG[city]
                city_norm = _normalise_str(city)

                # Selector de actividad económica
                ciiu_opts = top_3_ciiu_principal[
                    top_3_ciiu_principal["city"].apply(_normalise_str).str.contains(city_norm)
                ]["ciiu_principal"].tolist()
                ciiu_labels = [format_ciiu(c) for c in ciiu_opts]

                col_filter, col_toggle = st.columns([4, 1])
                with col_filter:
                    select_options = ["Todas", "Top 3"] + ciiu_labels
                    selected_label = st.selectbox(
                        "Actividad económica (CIIU)",
                        select_options,
                        index=0,
                        key=f"ciiu_sel_{city}",
                    )

                with col_toggle:
                    show_rues = st.checkbox(
                        "Solo RUES",
                        value=True,
                        key=f"show_rues_{city}",
                    )

                # Determine filter codes
                if selected_label == "Todas":
                    codes_filter = None
                elif selected_label == "Top 3":
                    codes_filter = ciiu_opts
                else:
                    label_to_code = {format_ciiu(c): c for c in ciiu_opts}
                    codes_filter = [label_to_code[selected_label]]

                # Prepare data
                zasca_plot_df = zasca_coords.copy()
                rues_plot_df = rues_coords.copy()

                # Apply activity filter
                if codes_filter is not None:
                    zasca_plot_df = zasca_plot_df[
                        zasca_plot_df["ciiu_principal"].isin(codes_filter)
                    ]
                    rues_plot_df = rues_plot_df[
                        rues_plot_df["ciiu_principal"].isin(codes_filter)
                    ]

                # Colour column
                zasca_plot_df["colour"] = zasca_plot_df["in_rues"].map(
                    lambda m: CLR_ZASCA_DARK if m else CLR_ZASCA_LIGHT
                )

                # Layers
                layer_zasca = pdk.Layer(
                    "ScatterplotLayer",
                    data=zasca_plot_df,
                    get_position="[longitude, latitude]",
                    get_fill_color="colour",
                    get_radius=60,
                    pickable=True,
                )
                layer_rues = make_layer(rues_plot_df, CLR_RUES)

                layers = ([layer_rues] if show_rues else []) + [layer_zasca]

                deck = pdk.Deck(
                    map_style="mapbox://styles/mapbox/light-v9",
                    initial_view_state=pdk.ViewState(
                        latitude=cfg["center"][0],
                        longitude=cfg["center"][1],
                        zoom=cfg["zoom"],
                        pitch=0,
                    ),
                    layers=layers,
                    tooltip={
                        "html": (
                            "<div style='font-family: Arial, sans-serif; font-size: 12px;'>"
                            "<b>ID:</b> {id}<br/>"
                            "<b>NIT:</b> {nit}<br/>"
                            "<b>CIIU:</b> {ciiu_principal}<br/>"
                            "<b>Dirección:</b> {gmaps_address}</div>"
                        ),
                        "style": {"backgroundColor": "#fefefe", "color": "#333333"},
                    },
                )

                col_map, col_side = st.columns([1, 1], gap="medium")

                with col_map:
                    st.subheader(city)
                    st.pydeck_chart(deck, use_container_width=True, height=800)

                with col_side:
                    st.subheader("Detalles")
                    st.caption(
                        "Unidades productivas en RUES (con presencia en ZASCA vs. sin ZASCA)"
                    )

                    city_df = rues_filtered[
                        rues_filtered["city"].apply(_normalise_str).str.contains(city_norm)
                    ]
                    if codes_filter is not None:
                        city_df = city_df[city_df["ciiu_principal"].isin(codes_filter)]

                    fig = build_density_plot(
                        city_df,
                        [
                            "empleados",
                            "activos_total",
                            "cantidad_mujeres_empleadas",
                            "ingresos_actividad_ordinaria",
                        ],
                    )
                    st.plotly_chart(
                        fig, use_container_width=True, key=f"plot_rues_{city}"
                    )

                    # ZASCA plot
                    st.caption(
                        "Unidades productivas en ZASCA (presencia en RUES vs. sin RUES)"
                    )

                    zasca_city_df = zasca_coords[
                        zasca_coords["city_zasca"].apply(_normalise_str).str.contains(city_norm)
                    ]
                    fig2 = build_density_plot_zasca(
                        zasca_city_df,
                        ["sales2022s", "emp_total"],
                    )
                    st.plotly_chart(
                        fig2, use_container_width=True, key=f"plot_zasca_{city}"
                    )

    with strategies_tab:
        st.header("Estrategias")
        st.markdown(
            "Próximamente se detallarán las estrategias correspondientes a cada cohorte."
        )


if __name__ == "__main__":
    main()
