import pydeck as pdk
import pandas as pd
from constants import CLR_RUES


def make_layer(df: pd.DataFrame, colour: str | list[int] | None = None, radius: int = 100) -> pdk.Layer:
    """
    Return a ScatterplotLayer for *df* with the given RGBA colour.

    Args:
        df: The DataFrame to create the layer for
        colour: The colour to use for the layer
        radius: The radius of the dots

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
        get_radius=radius,
        get_line_color=[0, 0, 0, 255],  # Black border
        get_line_width=2,  # Thin black border
        pickable=True,
    )
