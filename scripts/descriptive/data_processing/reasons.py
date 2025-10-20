"""Build comparable reasons-for-entrepreneurship dataset for ZASCA and EMICRON."""

from __future__ import annotations

import logging

import pandas as pd

from data_processing.utils import apply_sector_filter

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.reasons")


# shared comparable categories
# - NECESIDAD: need/no alternative income; includes job loss in ZASCA
# - OPORTUNIDAD: identified opportunity of steady income
# - TRADICION: cedido/regalado/heredado (inheritance/tradition)
COMPARABLE = ["Necesidad", "Oportunidad", "Tradición/Herencia", "Other"]


def _map_zasca_reason(reason: str | None) -> str | None:
    if not isinstance(reason, str):
        return None
    r = reason.strip().lower()
    # necesidad
    necesidad_terms = (
        "necesito",
        "necesitaba",
        "necesidad",
        "pérdida de empleo",
        "perdida de empleo",
        "reducción de ingresos",
        "reduccion de ingresos",
    )
    if any(term in r for term in necesidad_terms):
        return "Necesidad"
    # oportunidad
    if "oportunidad" in r:
        return "Oportunidad"
    # tradición/herencia
    if "cedieron" in r or "regalaron" in r or "hered" in r:
        return "Tradición/Herencia"
    # everything else → Other
    return "Other"


def _map_emicron_reason(code: float | None) -> str | None:
    if code is None:
        return None
    try:
        c = int(code)
    except Exception:  # noqa: BLE001
        return None
    if c == 1:  # no tiene otra alternativa de ingresos
        return "Necesidad"
    if c == 2:  # noqa: PLR2004 # oportunidad de negocio
        return "Oportunidad"
    if c == 3:  # noqa: PLR2004 # radición familiar / heredó
        return "Tradición/Herencia"
    # Other EMICRON categories (4,5,6,7) → Other
    return "Other"


@apply_sector_filter
def reasons(df_zasca: pd.DataFrame, df_emicron_2024_merged: pd.DataFrame) -> pd.DataFrame:
    """Create reasons dataset with common/excess parts per category (mirror-ready).

    Returns columns: category, type, value, source, plot_value, color_category.

    Args:
        df_zasca: ZASCA dataframe
        df_emicron_2024_merged: EMICRON dataframe

    Returns:
        pd.DataFrame: processed reasons dataset with common/excess parts per category (mirror-ready)

    """
    # ZASCA proportions
    zasca_prop = (
        df_zasca[["reason2start"]]
        .assign(category=lambda d: d["reason2start"].apply(_map_zasca_reason))
        .dropna(subset=["category"])
        .loc[lambda d: d["category"].isin(COMPARABLE), "category"]
        .value_counts()
        .reindex(COMPARABLE, fill_value=0)
        .pipe(lambda s: (s / s.sum()).rename("proportion").reset_index())
        .assign(source="ZASCA")
    )

    # EMICRON proportions
    em_prop = (
        df_emicron_2024_merged[["P3051", "F_EXP"]]
        .assign(category=lambda d: d["P3051"].apply(_map_emicron_reason))
        .dropna(subset=["category", "F_EXP"])
        .loc[lambda d: d["category"].isin(COMPARABLE)]
        .groupby("category", observed=True)["F_EXP"]
        .sum()
        .reindex(COMPARABLE, fill_value=0.0)
        .pipe(lambda s: (s / float(s.sum())).rename("proportion").reset_index())
        .assign(source="EMICRON")
    )

    base = pd.concat([zasca_prop, em_prop], ignore_index=True).assign(
        category=lambda d: pd.Categorical(d["category"], categories=COMPARABLE, ordered=True)
    )

    # compute common and excess per category
    wide = base.pivot_table(index="category", columns="source", values="proportion", fill_value=0.0, observed=True)
    wide = wide.reindex(index=COMPARABLE)
    common = wide.min(axis=1)
    zasca_excess = (wide.get("ZASCA", 0.0) - common).clip(lower=0.0)
    emicron_excess = (wide.get("EMICRON", 0.0) - common).clip(lower=0.0)

    parts = pd.concat([
        pd.DataFrame({"category": wide.index, "type": "common", "value": common}),
        pd.DataFrame({"category": wide.index, "type": "ZASCA_excess", "value": zasca_excess}),
        pd.DataFrame({"category": wide.index, "type": "EMICRON_excess", "value": emicron_excess}),
    ]).reset_index(drop=True)

    # duplicate common for both sources; assign sources to excess rows
    common_rows = parts.loc[parts["type"] == "common"].copy()
    common_zasca = common_rows.copy()
    common_zasca["source"] = "ZASCA"
    common_emicron = common_rows.copy()
    common_emicron["source"] = "EMICRON"
    excess_rows = parts.loc[parts["type"] != "common"].copy()
    excess_rows["source"] = excess_rows["type"].str.replace("_excess", "", regex=False)

    final_df = pd.concat([common_zasca, common_emicron, excess_rows], ignore_index=True)
    final_df = final_df.loc[final_df["value"] > 0].copy()

    # mirror-ready helpers (scale to percentage for plot)
    final_df["plot_value"] = final_df.apply(
        lambda r: -r["value"] * 100 if r["source"] == "ZASCA" else r["value"] * 100, axis=1
    )
    final_df["color_category"] = final_df["source"] + "_" + final_df["type"].str.replace("_excess", "", regex=False)

    return final_df.sort_values(["category", "source", "type"]).reset_index(drop=True)
