import pydeck as pdk
import pandas as pd
from constants import CLR_RUES


def make_layer(df: pd.DataFrame, colour: list[int] | None = None) -> pdk.Layer:
    """
    Return a ScatterplotLayer for *df* with the given RGBA colour.

    Args:
        df: The DataFrame to create the layer for
        colour: The colour to use for the layer

    Returns:
        pdk.Layer: The ScatterplotLayer

    """
    if colour is None:
        colour = CLR_RUES

    return pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position="[longitude, latitude]",
        get_fill_color=colour,
        get_radius=60,
        pickable=True,
    )
