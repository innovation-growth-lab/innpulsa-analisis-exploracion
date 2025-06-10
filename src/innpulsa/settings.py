"""
Global settings and configuration
"""

import os
from pathlib import Path

# Get the project root directory (two levels up from this file)
ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = os.path.join(ROOT_DIR, "../../10_Insumos evaluación impacto/")
DATA_DIR = os.path.join(ROOT_DIR, "../data/")
