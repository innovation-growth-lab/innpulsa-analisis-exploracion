"""Business formality indicators: RUT, bookkeeping, and credit access for ZASCA and EMICRON."""

from __future__ import annotations

import logging

import pandas as pd

from data_processing.utils import apply_sector_filter

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.formality")


def _map_zasca_rut(value: str | None) -> str | None:
    """Map ZASCA RUT responses to Sí/No.

    Args:
        value: ZASCA RUT response

    Returns:
        str: Sí/No

    """
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    if "sí" in v or "si" in v:
        return "Sí"
    if "no" in v:
        return "No"
    return None


def _map_emicron_rut(value: float | None) -> str | None:
    """Map EMICRON P1633 to Sí/No.

    Args:
        value: EMICRON P1633 response

    Returns:
        str: Sí/No

    """
    if value is None:
        return None
    try:
        c = int(value)
    except Exception:  # noqa: BLE001
        return None
    if c == 1:
        return "Sí"
    if c == 2:  # noqa: PLR2004
        return "No"
    return None


def _map_zasca_bookkeeping(value: str | None) -> str | None:  # noqa: PLR0911
    """Map ZASCA bookkeeping to standardized categories.

    Args:
        value: ZASCA bookkeeping response

    Returns:
        str: standardized category

    """
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    # keeps in head
    if "cabeza" in v:
        return "Cuentas en la cabeza"
    # no records at all
    if "ninguno" in v or "no lleva" in v:
        return "No lleva registros"
    # formal books
    if "libro contable" in v:
        return "Libros contables formales"
    # excel/software (merged category)
    if "excel" in v or "programa" in v or "otro" in v or "software" in v:
        return "Excel/Software"
    # manual/informal (only a mano)
    if "mano" in v:
        return "Registros informales"
    # accountant - filter out (return None)
    if "contador" in v:
        return None
    return None


def _map_emicron_bookkeeping(value: float | None) -> str | None:  # noqa: PLR0911
    """Map EMICRON P640 to standardized categories.

    Args:
        value: EMICRON P640 response

    Returns:
        str: standardized category

    """
    if value is None:
        return None
    try:
        c = int(value)
    except Exception:  # noqa: BLE001
        return None
    if c == 5:  # noqa: PLR2004
        return "No lleva registros"
    if c == 4:  # noqa: PLR2004
        return None  # filter out accountant
    if c in {1, 2}:
        return "Libros contables formales"
    if c == 3:  # noqa: PLR2004
        return "EMICRON_MIXED"  # will be split into Registros informales and Excel/Software
    return None


def _map_zasca_credit(value: str | None) -> str | None:
    """Map ZASCA hascredit to Sí/No.

    Args:
        value: ZASCA hascredit response

    Returns:
        str: Sí/No

    """
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    if "sí" in v or "si" in v:
        return "Sí"
    if "no" in v:
        return "No"
    return None


def _map_emicron_credit(value: float | None) -> str | None:
    """Map EMICRON P1765 to Sí/No.

    Args:
        value: EMICRON P1765 response

    Returns:
        str: Sí/No

    """
    if value is None:
        return None
    try:
        c = int(value)
    except Exception:  # noqa: BLE001
        return None
    if c == 1:
        return "Sí"
    if c == 2:  # noqa: PLR2004
        return "No"
    return None


@apply_sector_filter
def formality(df_zasca: pd.DataFrame, df_emicron_2024_merged: pd.DataFrame) -> pd.DataFrame:  # noqa: PLR0914
    """Create formality indicators dataset for ZASCA and EMICRON.

    Processes three indicators: RUT registration, bookkeeping practices, and credit access.

    Args:
        df_zasca: ZASCA dataframe
        df_emicron_2024_merged: EMICRON dataframe

    Returns:
        pd.DataFrame: processed formality data with columns
                     ['indicator','category','proportion','source']

    """
    logger.info("processing formality indicators")

    results = []

    # RUT registration
    zasca_rut = (
        df_zasca[["rut"]]
        .assign(category=lambda d: d["rut"].apply(_map_zasca_rut))
        .dropna(subset=["category"])
        .loc[:, "category"]
        .value_counts(normalize=True)
        .rename("proportion")
        .reset_index()
        .assign(source="ZASCA", indicator="Registro RUT")
    )

    em_rut = (
        df_emicron_2024_merged[["P1633", "F_EXP"]]
        .assign(category=lambda d: d["P1633"].apply(_map_emicron_rut))
        .dropna(subset=["category", "F_EXP"])
        .groupby("category", observed=True)["F_EXP"]
        .sum()
        .pipe(lambda s: (s / s.sum()).rename("proportion").reset_index())
        .assign(source="EMICRON", indicator="Registro RUT")
    )

    results.extend([zasca_rut, em_rut])

    # Bookkeeping practices - ZASCA
    zasca_book = (
        df_zasca[["bookkeeping"]]
        .assign(category=lambda d: d["bookkeeping"].apply(_map_zasca_bookkeeping))
        .dropna(subset=["category"])
        .loc[:, "category"]
        .value_counts(normalize=True)
        .rename("proportion")
        .reset_index()
        .assign(source="ZASCA", indicator="Contabilidad")
    )

    # calculate split ratios from ZASCA
    zasca_totals = zasca_book.set_index("category")["proportion"]

    # split 1: "Registros informales" (manual) vs "Excel/Software" for EMICRON category 3
    informal = zasca_totals.get("Registros informales", 0)
    excel_software = zasca_totals.get("Excel/Software", 0)
    split1_total = informal + excel_software
    mixed_ratio = (informal / split1_total, excel_software / split1_total) if split1_total > 0 else (0.5, 0.5)

    # split 2: "No lleva registros" vs "Cuentas en la cabeza" for EMICRON category 5
    no_records = zasca_totals.get("No lleva registros", 0)
    in_head = zasca_totals.get("Cuentas en la cabeza", 0)
    split2_total = no_records + in_head
    no_records_ratio = (no_records / split2_total, in_head / split2_total) if split2_total > 0 else (0.5, 0.5)

    # Bookkeeping practices - EMICRON
    em_weights = (
        df_emicron_2024_merged[["P640", "F_EXP"]]
        .assign(category=lambda d: d["P640"].apply(_map_emicron_bookkeeping))
        .dropna(subset=["category", "F_EXP"])
        .groupby("category", observed=True)["F_EXP"]
        .sum()
    )
    total_w = em_weights.sum()

    # expand splits for EMICRON
    expanded = []
    for cat, weight in em_weights.items():
        prop = weight / total_w
        if cat == "EMICRON_MIXED":
            # split category 3 into manual vs Excel/Software
            expanded.extend([
                {"category": "Registros informales", "proportion": prop * mixed_ratio[0]},
                {"category": "Excel/Software", "proportion": prop * mixed_ratio[1]},
            ])
        elif cat == "No lleva registros":
            # split category 5 into no records vs in head
            expanded.extend([
                {"category": "No lleva registros", "proportion": prop * no_records_ratio[0]},
                {"category": "Cuentas en la cabeza", "proportion": prop * no_records_ratio[1]},
            ])
        else:
            expanded.append({"category": cat, "proportion": prop})

    em_book = pd.DataFrame(expanded).assign(source="EMICRON", indicator="Contabilidad")
    results.extend([zasca_book, em_book])

    # Credit access
    zasca_credit = (
        df_zasca[["hascredit"]]
        .assign(category=lambda d: d["hascredit"].apply(_map_zasca_credit))
        .dropna(subset=["category"])
        .loc[:, "category"]
        .value_counts(normalize=True)
        .rename("proportion")
        .reset_index()
        .assign(source="ZASCA", indicator="Acceso a Crédito")
    )

    em_credit = (
        df_emicron_2024_merged[["P1765", "F_EXP"]]
        .assign(category=lambda d: d["P1765"].apply(_map_emicron_credit))
        .dropna(subset=["category", "F_EXP"])
        .groupby("category", observed=True)["F_EXP"]
        .sum()
        .pipe(lambda s: (s / s.sum()).rename("proportion").reset_index())
        .assign(source="EMICRON", indicator="Acceso a Crédito")
    )

    results.extend([zasca_credit, em_credit])

    combined = pd.concat(results, ignore_index=True)
    return combined.loc[:, ["indicator", "category", "proportion", "source"]]
