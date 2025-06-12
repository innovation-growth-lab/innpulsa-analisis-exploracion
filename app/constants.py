from pathlib import Path

# Paths to data
DATA_DIR = Path("data/processed/geolocation")

ZASCA_ADDRESSES_PATH = DATA_DIR / "zasca_addresses.csv"
ZASCA_COORDS_PATH = DATA_DIR / "zasca_coordinates.csv"
ZASCA_TOTAL_PATH = DATA_DIR / "../zasca_total.csv"
RUES_COORDS_PATH = DATA_DIR / "rues_coordinates.csv"
RUES_FILTERED_PATH = DATA_DIR / "rues_total_merged.csv"

# City map settings
CITY_CONFIG: dict[str, dict[str, tuple | int]] = {
    "Cúcuta": {"center": (7.889, -72.505), "zoom": 12},
    "Medellín": {"center": (6.244, -75.574), "zoom": 12},
    "Bucaramanga": {"center": (7.119, -73.122), "zoom": 12},
}

# Colours
# RGBA colours (R, G, B, A)
CLR_ZASCA_LIGHT = [50, 205, 50, 180]  # light-green  (ZASCA only)
CLR_ZASCA_DARK = [0, 100, 0, 200]  # dark-green   (ZASCA + RUES)
CLR_RUES = [255, 0, 0, 160]  # red (Solo RUES)

# CIIU descriptions (extend as needed)
CIIU_DESCRIPTIONS: dict[int, str] = {
    1521: "Fabricación de calzado de cuero y piel, con cualquier tipo de suela",
    1410: "Confección de prendas de vestir, excepto prendas de piel",
    4772: "Comercio al por menor de calzado y artículos de cuero y sucedáneos del cuero en establecimientos especializados",
    1522: "Fabricación de otros tipos de calzado, excepto de cuero y piel",
    1313: "Acabado de productos textiles",
    4642: "Comercio al por mayor de prendas de vestir",
}
