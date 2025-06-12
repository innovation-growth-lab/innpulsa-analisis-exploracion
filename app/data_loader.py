from __future__ import annotations

import pandas as pd

from constants import (
    ZASCA_ADDRESSES_PATH,
    ZASCA_COORDS_PATH,
    ZASCA_TOTAL_PATH,
    RUES_COORDS_PATH,
    RUES_FILTERED_PATH,
)


def _normalise_str(s: str) -> str:
    """Return lowercase ASCII-only version of *s* (remove accents)."""
    import unicodedata

    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        if isinstance(s, str)
        else ""
    )


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load, merge and clean the data used by the application.

    Returns
    -------
    rues_filtered : DataFrame
    top_3_ciiu_principal : DataFrame
    zasca_coords : DataFrame
    rues_coords : DataFrame
    """

    zasca_addresses = pd.read_csv(
        ZASCA_ADDRESSES_PATH, encoding="utf-8-sig", index_col=0
    )
    zasca_coords = pd.read_csv(ZASCA_COORDS_PATH, encoding="utf-8-sig", index_col=0)
    rues_coords = pd.read_csv(RUES_COORDS_PATH, encoding="utf-8-sig", index_col=0)
    rues_filtered = pd.read_csv(RUES_FILTERED_PATH, encoding="utf-8-sig", index_col=0)
    zasca_total = pd.read_csv(ZASCA_TOTAL_PATH, encoding="utf-8-sig")

    # enrich RUES coordinates with business info
    rues_coords = rues_coords.merge(
        rues_filtered,
        left_on="id",
        right_on="nit",
        how="left",
    )

    # identify the top 3 ciiu_principal per city among in_rues == True
    top_3_ciiu_principal = (
        rues_filtered[
            rues_filtered["in_rues"]
            & rues_filtered["city"].isin(["Medellin", "Cucuta", "Bucaramanga"])
        ]
        .groupby(["ciiu_principal", "city"])  # type: ignore[arg-type]
        .size()
        .reset_index(name="count")
        .sort_values(["city", "count"], ascending=[True, False])
        .groupby("city")
        .head(3)
    )

    # merge ZASCA addresses
    zasca_coords = zasca_coords.merge(
        zasca_addresses.reset_index()[["id", "nit", "city"]],
        on="id",
        how="left",
        suffixes=("", "_zasca"),
    )
    # fix city names
    zasca_coords["city_zasca"] = zasca_coords["city_zasca"].replace(
        "San José de Cúcuta", "Cúcuta"
    )

    # merge rues info into ZASCA coords
    zasca_coords = zasca_coords.merge(rues_filtered, on="nit", how="left")

    # add totals info
    zasca_coords = zasca_coords.merge(
        zasca_total[["numberid_emp1", "sales2022s", "emp_total"]],
        left_on="id",
        right_on="numberid_emp1",
        how="left",
    )

    # ensure proper dtypes
    zasca_coords.drop_duplicates(subset=["id"], inplace=True)
    zasca_coords["in_rues"] = zasca_coords["in_rues"].fillna(False)

    # convert lat / lon to numeric and drop NAs
    for df in (zasca_coords, rues_coords):
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df.dropna(subset=["latitude", "longitude"], inplace=True)

    return rues_filtered, top_3_ciiu_principal, zasca_coords, rues_coords
