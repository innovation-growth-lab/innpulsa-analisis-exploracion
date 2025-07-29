"""Streamlit entrypoint for ZASCA & RUES visualisation.

Run with:
    streamlit run app/app.py
"""

from __future__ import annotations

import streamlit as st
import streamlit.components.v1 as components
import pydeck as pdk
import pandas as pd
from pathlib import Path

from constants import (
    CITY_CONFIG,
    CLR_ZASCA_DARK,
    CLR_ZASCA_LIGHT,
    CLR_RUES,
    MAPBOX_API_KEY,
)
from data_loader import load_data, normalise_str
from plots import (
    build_density_plot,
    build_density_plot_zasca,
    format_ciiu,
)
from map_components import make_layer


@st.cache_data(show_spinner="Cargando datos...")
def get_data():
    """
    Get data from the data loader.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
            rues_filtered, top_3_ciiu_principal, zasca_coords, rues_coords

    """
    return load_data()


def prepare_city_filters(city, top_3_ciiu_principal):
    """Prepare filter options and UI for a city tab.

    Args:
        city: The city to prepare filters for
        top_3_ciiu_principal: The top 3 CIIU principal for the city

    Returns:
        tuple[list[int], bool, str]: The filter options and UI for the city tab

    """
    citynorm = normalise_str(city)

    # Selector de actividad económica
    ciiu_opts = top_3_ciiu_principal[top_3_ciiu_principal["city"].apply(normalise_str).str.contains(citynorm)][
        "ciiu_principal"
    ].tolist()
    ciiu_labels = [format_ciiu(c) for c in ciiu_opts]

    col_filter, col_toggle = st.columns([4, 1])
    with col_filter:
        select_options = ["Todas", "Top 3", *ciiu_labels]
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

    return codes_filter, show_rues, citynorm


def prepare_map_data(zasca_coords, rues_coords, codes_filter):
    """Prepare and filter map data based on selected filters.

    Args:
        zasca_coords: The ZASCA coordinates
        rues_coords: The RUES coordinates
        codes_filter: The codes to filter the data by

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: The filtered data

    """
    zasca_plot_df = zasca_coords.copy()
    rues_plot_df = rues_coords.copy()

    # Apply activity filter
    if codes_filter is not None:
        zasca_plot_df = zasca_plot_df[zasca_plot_df["ciiu_principal"].isin(codes_filter)]
        rues_plot_df = rues_plot_df[rues_plot_df["ciiu_principal"].isin(codes_filter)]

    # Colour column
    zasca_plot_df["colour"] = zasca_plot_df["in_rues"].map(lambda m: CLR_ZASCA_DARK if m else CLR_ZASCA_LIGHT)

    return zasca_plot_df, rues_plot_df


def render_city_map(city, cfg, zasca_plot_df, rues_plot_df, show_rues):
    """Render the map component for a city.

    Args:
        city: The city to render the map for
        cfg: The configuration for the city
        zasca_plot_df: The ZASCA data
        rues_plot_df: The RUES data
        show_rues: Whether to show the RUES layer

    """
    # Layers
    layer_zasca = pdk.Layer(
        "ScatterplotLayer",
        data=zasca_plot_df,
        get_position="[longitude, latitude]",
        get_fill_color="colour",
        get_radius=60,
        pickable=True,
    )
    layer_rues = make_layer(pd.DataFrame(rues_plot_df), CLR_RUES)

    layers = ([layer_rues] if show_rues else []) + [layer_zasca]

    deck = pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v10",
        api_keys={"mapbox": MAPBOX_API_KEY},
        initial_view_state=pdk.ViewState(
            latitude=cfg["center"][0],
            longitude=cfg["center"][1],
            zoom=cfg["zoom"],
            pitch=0,
        ),
        layers=layers,
        tooltip={  # type: ignore[reportArgumentType]
            "html": (
                "<div style='font-family: Arial, sans-serif; font-size: 12px;'>"
                "<b>ID:</b> {id}<br/>"
                "<b>NIT:</b> {nit}<br/>"
                "<b>CIIU:</b> {ciiu_principal}<br/>"
                "<b>Dirección:</b> {gmaps_address}</div>"
            ),
            "style": {
                "backgroundColor": "#fefefe",
                "color": "#333333",
            },
        },
    )

    st.subheader(city)
    st.pydeck_chart(deck, use_container_width=True, height=800)


def render_city_plots(city, citynorm, rues_filtered, zasca_coords, codes_filter):
    """Render the plot components for a city."""
    st.subheader("Detalles")
    st.caption("Unidades productivas en RUES (con presencia en ZASCA vs. sin ZASCA)")

    city_df = rues_filtered[rues_filtered["city"].apply(normalise_str).str.contains(citynorm)]
    if codes_filter is not None:
        city_df = city_df[city_df["ciiu_principal"].isin(codes_filter)]

    fig = build_density_plot(
        pd.DataFrame(city_df),
        [
            "empleados",
            "activos_total",
            "cantidad_mujeres_empleadas",
            "ingresos_actividad_ordinaria",
        ],
    )
    st.plotly_chart(fig, use_container_width=True, key=f"plot_rues_{city}")

    # ZASCA plot
    st.caption("Unidades productivas en ZASCA (presencia en RUES vs. sin RUES)")

    zasca_city_df = zasca_coords[zasca_coords["city_zasca"].apply(normalise_str).str.contains(citynorm)]
    fig2 = build_density_plot_zasca(
        pd.DataFrame(zasca_city_df),
        ["sales2022s", "emp_total"],
    )
    st.plotly_chart(
        fig2,
        use_container_width=True,
        key=f"plot_zasca_{city}",
    )


def render_city_tab(city, cfg, data_tuple):
    """Render a single city tab with map and plots."""
    rues_filtered, top_3_ciiu_principal, zasca_coords, rues_coords = data_tuple

    codes_filter, show_rues, citynorm = prepare_city_filters(city, top_3_ciiu_principal)
    zasca_plot_df, rues_plot_df = prepare_map_data(zasca_coords, rues_coords, codes_filter)

    col_map, col_side = st.columns([1, 1], gap="medium")

    with col_map:
        render_city_map(city, cfg, zasca_plot_df, rues_plot_df, show_rues)

    with col_side:
        render_city_plots(city, citynorm, rues_filtered, zasca_coords, codes_filter)


def render_map_tabs(data_tuple):
    """Render all city map tabs."""
    city_tabs = st.tabs(list(CITY_CONFIG.keys()))

    for city, tab in zip(CITY_CONFIG.keys(), city_tabs, strict=True):
        with tab:
            cfg = CITY_CONFIG[city]
            render_city_tab(city, cfg, data_tuple)


def render_strategy_iv():
    """Render the IV strategy section."""
    with st.expander(
        "Posibilidad A: Variable instrumental (IV) con distancia al centro",
        expanded=False,
    ):
        col_img, col_content = st.columns([1, 4])
        with col_img:
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image

            st.image("data/iv.png", use_container_width=True)
        with col_content:
            st.subheader("Descripción y lógica")
            st.markdown(
                """
                Se utiliza la **distancia** de la unidad productiva al centro ZASCA como una variable instrumental,
                 siguiendo el enfoque de [McKenzie y Sakho (2010)](https://econpapers.repec.org/article/eeedeveco/v_3a91_3ay_3a2010_3ai_3a1_3ap_3a15-24.htm).
                 La idea es que una mayor distancia aumenta los costos de acceso (tiempo, transporte) y, por tanto,
                 reduce la probabilidad de que una empresa participe en el programa.
                """
            )

            st.info(
                """
                **📌 Supuesto clave de exclusión (Crítico):** La ubicación del centro **NO** debe estar
                 correlacionada con el potencial económico de la zona para el sector que apoya. Es decir,
                 la distancia solo debe afectar los resultados de la empresa (ej. ingresos) **a través**
                 de su efecto sobre la participación en ZASCA, y no por otras vías (ej. por estar en una
                 zona con más o menos dinamismo económico).
                """
            )

            col1, col2 = st.columns([2, 2])
            with col1:
                st.success("#### ✅ Ventajas")
                st.markdown(
                    """
                - **Intuitivo:** La relación entre distancia y costo de acceso es fácil de entender.
                - **Efecto causal:** Si el supuesto se cumple, permite estimar el efecto causal del programa para el
                 subgrupo de empresas cuya participación es sensible a la distancia.
                """
                )
            with col2:
                st.warning("#### ⚠️ Desafíos y riesgos")
                st.markdown(
                    """
                - **Supuesto fuerte:** Es muy difícil de defender. La ubicación de los centros puede ser estratégica
                 y no aleatoria.
                - **Instrumento débil:** Para ciertos sectores (ej. agroindustria, servicios digitales), la
                 distancia física puede ser un factor poco relevante.
                - **Violación de exclusión:** Si el centro ZASCA está co-localizado con otros servicios (Cámara de
                 Comercio, etc.), la distancia captura el acceso a todo ese ecosistema, no solo a ZASCA.
                """
                )

            st.subheader("Datos y variables clave")
            st.markdown(
                """
            - **Fuentes:** RUES para muestra de control y resultados, datos del programa para localización de
             centros y participantes.
            - **Muestra:** Unidades productivas pre-seleccionadas y estratificadas (ej. por CIIU).
            - **Variables:** Localización (lat/lon), participación en ZASCA, empleo (general y femenino), activos,
             ingresos.
            """
            )


def render_strategy_did_centers():
    """Render the DiD between centers strategy section."""
    with st.expander("Posibilidad B.1: Difference-in-differences (Entre centros / Despliegue escalonado)"):
        col_img, col_content = st.columns([1, 4])
        with col_img:
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image

            st.image("data/did1.png", use_container_width=True)
        with col_content:
            st.subheader("Descripción y lógica")
            st.markdown(
                """
            Esta estrategia aprovecha que los centros ZASCA se abren en diferentes momentos y lugares. Compara el
             cambio en los resultados de las empresas en municipios que reciben un centro ("grupo de tratamiento")
              con el cambio en los resultados de empresas en municipios donde los centros se abrirán más tarde
               ("grupo de control").
            """
            )

            st.info(
                """
            **📌 Supuesto clave (Tendencias paralelas):** En ausencia del programa ZASCA, los resultados de las
             empresas en ambos grupos de municipios (tratamiento y control) habrían seguido trayectorias o
              tendencias similares a lo largo del tiempo.
            """
            )

            col1, col2 = st.columns([2, 2])

            with col1:
                st.success("#### ✅ Ventajas")
                st.markdown(
                    """
                - **Robusto:** Considerada una de las estrategias cuasi-experimentales más sólidas.
                - **Aprovecha el diseño:** Usa una característica real del despliegue del programa (el
                 escalonamiento).
                - **Controla no observables:** Mitiga el sesgo de autoselección (ej. "willingness to attend") y
                 controla por factores fijos no observables de las empresas o municipios.
                """
                )
            with col2:
                st.warning("#### ⚠️ Desafíos y riesgos")
                st.markdown(
                    """
                - **Modelación compleja:** Los modelos de DiD escalonado (staggered) requieren consideraciones
                 técnicas cuidadosas.
                - **Heterogeneidad:** Hay que controlar por diferencias regionales (ej. `city linear trends`) y por
                 la variabilidad en la calidad del operador y los servicios entre los distintos centros.
                - **Validar supuesto:** Se necesita data de varios periodos pre-tratamiento para verificar la
                 plausibilidad de las tendencias paralelas.
                """
                )

            st.subheader("Datos y variables clave")
            st.markdown(
                """
            - **Fuentes:** RUES (idealmente como panel de datos), calendario oficial de apertura de centros.
            - **Variables:** Datos de resultados (empleo, ventas) a nivel de empresa/municipio en varios puntos del
             tiempo. Características municipales para controles.
            """
            )


def render_strategy_did_cohorts():
    """Render the DiD cohorts strategy section."""
    with st.expander("Posibilidad B.2: Difference-in-differences (Intra-centro / Cohortes)"):
        col_img, col_content = st.columns([1, 4])
        with col_img:
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image
            st.write("")  # Add some space above the image

            st.image("data/did2.png", use_container_width=True)
        with col_content:
            st.subheader("Descripción y lógica")
            st.markdown(
                """
            En lugar de comparar entre diferentes municipios, esta variante compara cohortes **dentro de un mismo
             centro**. Se comparan los resultados de las cohortes que entran antes ("early treated") con las que
              entran después ("later treated").
            """
            )

            st.info(
                """
            **📌 Supuesto clave (Asignación aleatoria a cohorte):** El momento en que una empresa decide entrar
             (en la primera o en la última cohorte) **no** debe estar correlacionado con sus características no
              observadas que afectan sus resultados (ej. motivación, urgencia, potencial de crecimiento).
            """
            )

            col1, col2 = st.columns([2, 2])
            with col1:
                st.success("#### ✅ Ventajas")
                st.markdown(
                    """
                - **Control perfecto:** Controla perfectamente por cualquier diferencia fija entre centros (calidad
                 del operador, ubicación, equipo disponible) al comparar empresas dentro del mismo entorno.
                - **Conceptualmente simple:** La comparación es muy directa si el supuesto se cumple.
                """
                )
            with col2:
                st.warning("#### ⚠️ Desafíos y riesgos")
                st.markdown(
                    """
                - **Modelación compleja:** Los modelos de DiD escalonado (staggered) requieren consideraciones
                 técnicas cuidadosas.
                - **Alto riesgo de sesgo:** Es muy probable que el supuesto no se cumpla. Las empresas más
                 motivadas, organizadas o con mayor urgencia podrían inscribirse en las primeras cohortes, lo
                  que contaminaría la comparación.
                """
                )

            st.subheader("Datos y variables clave")
            st.markdown(
                """
            - **Fuentes:** RUES (panel de datos), listas de participantes de ZASCA con identificadores precisos de
             **cohorte** y **centro**.
            - **Variables:** Idem que en B.1, pero con la necesidad crítica de tener el dato de la cohorte de
             inicio de cada participante.
            """
            )


def render_comparison_table():
    """Render the comparison summary table."""
    st.divider()

    # --- Tabla Resumen Final ---
    st.header("Tabla resumen comparativa")
    st.markdown("Una vista rápida de las tres opciones y sus características principales.")

    summary_data = {
        "Característica": [
            "Estrategia principal",
            "Grupo de control",
            "Supuesto más crítico",
            "Mayor riesgo",
            "Recomendación",
        ],
        "Opción A (IV con Distancia)": [
            "Variable instrumental",
            "Empresas similares que no participan (afectadas por la distancia)",
            "La ubicación del centro es exógena al potencial económico de la zona",
            "El supuesto de exclusión es muy poco creíble",
            "Baja viabilidad, alto riesgo de sesgo",
        ],
        "Opción B.1 (DiD Entre Centros)": [
            "Difference-in-differences (escalonado)",
            "Empresas en municipios donde el centro abre más tarde",
            "Tendencias paralelas entre regiones tratamiento y control",
            "La heterogeneidad entre centros y regiones complica el modelo",
            "Alta viabilidad, es el estándar de oro cuasi-experimental",
        ],
        "Opción B.2 (DiD Intra-Centro)": [
            "Difference-in-differences (por cohortes)",
            "Cohortes que inician el programa más tarde en el mismo centro",
            "La entrada a una cohorte temprana vs. tardía es aleatoria",
            "Sesgo de autoselección (las empresas más motivadas entran primero)",
            "Muy baja viabilidad, alto riesgo de sesgo por autoselección",
        ],
    }

    summary_df = pd.DataFrame(summary_data)
    st.dataframe(summary_df, hide_index=True, use_container_width=True)


def render_technical_considerations():
    """Render the technical considerations section."""
    st.header("Pasos Adicionales y Consideraciones Técnicas")
    st.markdown("Para incrementar la robustez de la evaluación, se deben considerar los siguientes análisis avanzados:")

    st.subheader("Validación Empírica para la Estrategia IV (Opción A)")
    st.markdown(
        """
    Para fortalecer la credibilidad del instrumento de distancia, se deben realizar pruebas de falsificación y
     robustez:
    - **Prueba de Placebo Temporal:** Verificar si la distancia a un centro ZASCA "futuro" (que aún no ha sido
     anunciado ni construido) predice resultados de las empresas en el **pasado**. Un efecto significativo aquí
     invalidaría el instrumento, ya que sugeriría que la distancia está capturando características preexistentes
      de la zona y no el efecto del centro.
    - **Análisis de Attrition (desgaste de la muestra):** Analizar si la distancia al centro predice la probabilidad
     de que una empresa salga de la muestra de evaluación (ej. cierre o imposibilidad de seguimiento). Si las
      empresas más lejanas son más propensas a desaparecer de los registros, esto podría introducir un sesgo de
       selección en la muestra final.
    """
    )

    st.subheader("Refinamiento de la Estrategia DiD Escalonada (Opción B.1)")
    st.markdown(
        """
    Dado que los modelos tradicionales de DiD con efectos fijos bidireccionales (TWFE) pueden producir estimaciones
     sesgadas en diseños escalonados, es importante utilizar estimadores modernos y robustos.
    - **Implementación de Estimadores Avanzados:** Se recomienda implementar métodos como el de **Callaway y
     Sant'Anna (2021)** o similares (Borusyak et al., 2022; Goodman-Bacon, 2021). Estos enfoques manejan
      correctamente la heterogeneidad de los efectos del tratamiento a lo largo del tiempo y entre grupos que son
       tratados en diferentes momentos, proporcionando estimaciones más fiables y descompuestas del efecto promedio
        del tratamiento (ATT).
    """
    )


def render_analysis_report():
    """Render the panel analysis HTML report."""
    # Try to read the HTML file
    html_file_path = "analysis/01_iv/panel_analysis.html"
    with Path(html_file_path).open(mode="r", encoding="utf-8") as f:
        html_content = f.read()

    st.header("Análisis Panel - Diagnóstico de Variables ZASCA")
    st.markdown("Reporte completo del análisis de variables instrumentales y diferencias-en-diferencias.")

    # Display the HTML content
    components.html(html_content, height=1600, scrolling=True)



def render_strategies_tab():
    """Render the strategies comparison tab.

    Args:
        data_tuple: The data tuple

    """
    render_strategy_iv()
    render_strategy_did_centers()
    render_strategy_did_cohorts()
    render_comparison_table()
    render_technical_considerations()


def main() -> None:
    """
    Run the app.

    This function sets up the Streamlit app and displays the main content.

    """
    st.set_page_config(page_title="Cohortes ZASCA y RUES", layout="wide")
    st.title("Cohortes ZASCA y RUES")

    data = get_data()
    maps_tab, strategies_tab, analysis_tab = st.tabs(["Mapas", "Estrategias", "Análisis"])

    with maps_tab:
        render_map_tabs(data)
    with strategies_tab:
        render_strategies_tab()
    with analysis_tab:
        render_analysis_report()


if __name__ == "__main__":
    main()
