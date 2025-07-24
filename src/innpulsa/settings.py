"""Global settings and configuration."""

from pathlib import Path

# Get the project root directory (two levels up from this file)
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "../data/"
RAW_DATA_DIR = DATA_DIR / "innpulsa_raw/10_Insumos evaluación impacto/"
