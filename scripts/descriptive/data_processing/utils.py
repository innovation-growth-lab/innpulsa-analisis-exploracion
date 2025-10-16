from functools import wraps
import pandas as pd

GROUPO_SECTOR = {
    "manufactura": 3,
    "agropecuario": 1,
}

MICRO_EMPRESA_THRESHOLD = 10

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

        # filter emicron data before applying function
        if filtro_por_sector and len(args) >= 2:
            df_zasca, df_emicron = args[0], args[1]
            df_emicron = _filter_by_sector(df_emicron, filtro_por_sector)
            args = (df_zasca, df_emicron, *args[2:])

        # apply function
        result = func(*args, **kwargs)
        return result

    return wrapper
