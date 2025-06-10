"""Logging configuration for the project."""

import logging
import sys
from pathlib import Path

from innpulsa.settings import DATA_DIR


def configure_logger(name: str) -> logging.Logger:
    """Get a configured logger instance."""
    logger = logging.getLogger(name)

    if not logger.handlers:  # avoid duplicate handlers
        log_dir = Path(DATA_DIR) / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # File handler
        file_handler = logging.FileHandler(log_dir / f"{name}.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        logger.setLevel(logging.INFO)

    return logger
