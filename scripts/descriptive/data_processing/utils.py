import logging

from functools import wraps
import pandas as pd

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.utils")

GROUPO_SECTOR = {
    "manufactura": 3,
    "agro": 1,
}

CIIU_MANUFACTURA = [
    "10",
    "13",
    "14",
    "20",
    "22",
    "25",
    "28",
    "29",
    "31",
]

CIIU_AGRICULTURA = [
    "01",
    "02",
    "03",
]

DEP_CODIGO = {
    "ANTIOQUIA": 5,
    "ATLÁNTICO": 8,
    "BOGOTÁ, D.C.": 11,
    "BOLÍVAR": 13,
    "BOYACÁ": 15,
    "CALDAS": 17,
    "CAQUETÁ": 18,
    "CAUCA": 19,
    "CUNDINAMARCA": 25,
    "HUILA": 41,
    "LA GUAJIRA": 44,
    "MAGDALENA": 47,
    "NARIÑO": 52,
    "NORTE DE SANTANDER": 54,
    "QUINDÍO": 63,
    "RISARALDA": 66,
    "SANTANDER": 68,
    "TOLIMA": 73,
    "VALLE DEL CAUCA": 76,
}


MICRO_EMPRESA_THRESHOLD = 10
TUPLE_SIZE = 2


def _filter_by_sector(df: pd.DataFrame, sector: str) -> pd.DataFrame:
    return df.loc[df["GRUPOS12"] == GROUPO_SECTOR[sector]]


def apply_sector_filter(func):
    """Apply sector filtering to functions that accept filtro_por_sector parameter.

    Args:
        func: Function to apply sector filtering to.

    Returns:
        Function with sector filtering applied.

    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        # extract filtro_por_sector from kwargs
        filtro_por_sector = kwargs.pop("filtro_por_sector", None)

        if filtro_por_sector:
            # iterate through all arguments and apply sector filtering
            filtered_args = []
            for i, arg in enumerate(args):
                if isinstance(arg, pd.DataFrame) and "GRUPOS12" in arg.columns:
                    try:
                        filtered_df = _filter_by_sector(arg, filtro_por_sector)
                        filtered_args.append(filtered_df)
                        logger.debug("applied sector filter to argument %d", i)
                    except Exception as e:  # noqa: BLE001
                        logger.warning(
                            "Warning: Failed to filter argument %d for sector %s: %s", i, filtro_por_sector, e
                        )
                        filtered_args.append(arg)  # keep original if filtering fails
                else:
                    filtered_args.append(arg)  # keep non-DataFrame arguments unchanged
            args = tuple(filtered_args)

        # apply function
        return func(*args, **kwargs)

    return wrapper
