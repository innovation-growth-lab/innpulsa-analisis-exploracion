import logging

from functools import wraps
import pandas as pd

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.utils")

GROUPO_SECTOR = {
    "manufactura": 3,
    "agropecuario": 1,
}

DEP_CODIGO = {
    "ANTIOQUIA": 5,
    "ATLÁNTICO": 8,
    "BOGOTÁ, D.C.": 11,
    "BOLÍVAR": 13,
    "CALDAS": 17,
    "CUNDINAMARCA": 25,
    "LA GUAJIRA": 44,
    "NORTE DE SANTANDER": 54,
    "SANTANDER": 68,
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

        # filter both zasca and other data before applying function
        if filtro_por_sector and len(args) >= TUPLE_SIZE:
            df_zasca, df_other = args[0], args[1]
            df_zasca = _filter_by_sector(df_zasca, filtro_por_sector)
            try:
                df_other = _filter_by_sector(df_other, filtro_por_sector)
            except Exception as e:  # noqa: BLE001
                other_name = args[1].__class__.__name__
                logger.warning("Warning: Failed to filter %s data for sector %s: %s", other_name, filtro_por_sector, e)
            args = (df_zasca, df_other, *args[2:])
        else:
            # assume it's zasca
            df_zasca = _filter_by_sector(args[0], filtro_por_sector)
            args = (df_zasca, *args[1:])

        # apply function
        return func(*args, **kwargs)

    return wrapper
