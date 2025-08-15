from pathlib import Path

# Paths to data
DATA_DIR = Path("data/02_processed/geolocation")

# ZASCA addresses paths - dataset-specific
DATA_WITH_COORDS_PATH = DATA_DIR / "data_with_coords.csv"

# City map settings
CENTRO_CONFIG = {
    "Bucaramanga": {"center": [7.12, -73.1276], "zoom": 12},
    "Medellín": {"center": [6.2527, -75.5628], "zoom": 12},
    "Manrique": {"center": [6.2650487, -75.5536652], "zoom": 12},
    "Cúcuta": {"center": [7.89391, -72.50782], "zoom": 12},
    "20Julio": {"center": [4.5711143, -74.0943969], "zoom": 12},
    "Baranoa": {"center": [10.7966, -74.9150], "zoom": 12},
    "Cali Norte": {"center": [3.4516, -76.5320], "zoom": 12},
    "Cartagena": {"center": [10.3932, -75.4832], "zoom": 12},
    "Caucasia": {"center": [7.9832, -75.1982], "zoom": 12},
    "Ciudad Bolivar": {"center": [4.5795, -74.1574], "zoom": 12},
    "Manizales": {"center": [5.0630, -75.5028], "zoom": 12},
    "Riohacha": {"center": [11.5384, -72.9168], "zoom": 12},
    "Suba": {"center": [4.7208, -74.0748], "zoom": 12},
}

CENTRO_ZASCA_CONFIG = {
    "Bucaramanga": [7.1049364854763475, -73.12383197704348],
    "Manrique": [6.284881727521926, -75.54409932364932],
    "Medellín": [6.232088566149681, -75.56902649888393],
    "Cúcuta": [7.829409950541552, -72.46036608947021],
    "20Julio": [4.569429291819494, -74.09478949758527],
    "Baranoa": [10.803854499386958, -74.91244952786113],
    "Cali Norte": [3.4703660708293342, -76.53109251974698],
    "Cartagena": [10.408413725517383, -75.46504629117649],
    "Caucasia": [7.996741312367327, -75.19635027124215],
    "Ciudad Bolivar": [4.543213679818289, -74.1469410119057],
    "Manizales": [5.063846037654722, -75.50186555759247],
    "Riohacha": [11.539682147003058, -72.91511631324943],
    "Suba": [4.7461323779336295, -74.08267727408058],
}

# Colours
# RGBA colours (R, G, B, A)
CLR_ZASCA_LIGHT = [50, 205, 50, 180]  # light-green  (ZASCA only)
CLR_ZASCA_DARK = [0, 100, 0, 200]  # dark-green   (ZASCA + RUES)
CLR_RUES = [255, 0, 0, 160]  # red (Solo RUES)

# CIIU descriptions (extend as needed)
CIIU_DESCRIPTIONS: dict[int, str] = {
    1081: "Elaboración de productos de panadería",
    1313: "Acabado de productos textiles",
    1391: "Fabricación de tejidos de punto y ganchillo",
    1392: "Confección de artículos con materiales textiles, excepto prendas de vestir",
    1410: "Confección de prendas de vestir, excepto prendas de piel",
    1521: "Fabricación de calzado de cuero y piel, con cualquier tipo de suela",
    1522: "Fabricación de otros tipos de calzado, excepto de cuero y piel",
    1690: "Fabricación de otros productos de madera; fabricación de artículos de corcho, cestería y espartería",
    1811: "Actividades de impresión",
    3312: "Mantenimiento y reparación especializado de maquinaria y equipo",
    3830: "Recuperación de materiales",
    4642: "Comercio al por mayor de prendas de vestir",
    4719: "Comercio al por menor en establecimientos no especializados con surtido compuesto principalmente por productos diferentes de alimentos (víveres en general), bebidas y tabaco",
    4751: "Comercio al por menor de productos textiles en establecimientos especializados",
    4752: "Comercio al por menor de artículos de ferretería, pinturas y productos de vidrio en establecimientos especializados",
    4771: "Comercio al por menor de prendas de vestir y sus accesorios (incluye artículos de piel) en establecimientos especializados",
    4772: "Comercio al por menor de todo tipo de calzado y artículos de cuero y sucedáneos del cuero en establecimientos especializados",
    4774: "Comercio al por menor de otros productos nuevos en establecimientos especializados",
    5619: "Otros tipos de expendio de comidas preparadas n.c.p. (no clasificado previamente)",
    8299: "Otras actividades de servicio de apoyo a las empresas n.c.p.",
}

# mapbox api public key
MAPBOX_API_KEY = "pk.eyJ1IjoiYW1wdWRpYTE5IiwiYSI6ImNtODV0ejRwNDA1enoya3NjZGZ0NGoxMG0ifQ.jzHtCPNEDUWNu_OF-Ra-hg"
