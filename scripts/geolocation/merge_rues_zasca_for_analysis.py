"""
Script to merge RUES and ZASCA data.

This script reads data from both RUES and ZASCA sources and performs an inner merge
based on business identifiers.

Usage:
    python analysis/create_merged_data.py --zasca_dataset five_centers
    python analysis/create_merged_data.py --zasca_dataset closed
"""

import argparse
from pathlib import Path
import pandas as pd
import re

from innpulsa.settings import DATA_DIR
from innpulsa.logging import configure_logger

logger = configure_logger("geolocation.merge_rues_zasca")


def main() -> None:
    """
    Merge RUES and ZASCA data.

    This script will:
    - Read the RUES and ZASCA data
    - Merge the data
    - Save the results

    """
    # read data from both sources - rues
    rues_total = pd.read_csv(
        str(Path(DATA_DIR) / "02_processed/rues_total.csv"), encoding="utf-8-sig", low_memory=False
    )
    rues_coords = pd.read_csv(
        str(Path(DATA_DIR) / "02_processed/geolocation/rues_coordinates.csv"), encoding="utf-8-sig"
    )

    # read data from both sources - zasca
    zasca_total = pd.read_csv(str(Path(DATA_DIR) / "02_processed/zasca_total.csv"), encoding="utf-8-sig")
    zasca_coords = pd.read_csv(
        str(Path(DATA_DIR) / "02_processed/geolocation/zasca_coordinates.csv"), encoding="utf-8-sig"
    )

    # let's sort of assume that if nit is missing in zasca_total, it may be numberid_emp1
    zasca_total["nit"] = zasca_total["nit"].fillna(zasca_total["numberid_emp1"])

    # if str(nit) with last character removed matches numberid_emp1, then replace nit with numberid_emp1
    zasca_total["nit"] = zasca_total.apply(
        lambda row: row["numberid_emp1"] if str(row["nit"])[:-1] == str(row["numberid_emp1"]) else row["nit"], axis=1
    )

    # also remap specific nits
    zasca_total["nit"] = zasca_total["nit"].replace({
        "1020442629": "1036606519",
        "1090417350": "1005035428",
        "10987961739": "1098796173",
        "11070600400": "1107060040",
        "18217688": "88217688",
        "276054395": "27605439",
        "372749036": "37274903",
        "602815376": "60281537",
        "603198313": "60319831",
        "1017280244": "5297750",
    })

    # remove patterns like "-<digit>" or " <digit>" or " -<digit>" only if nit is a string and not null
    zasca_total["nit"] = zasca_total["nit"].apply(lambda x: x if pd.isna(x) else re.sub(r"([ -])\d", "", str(x)))

    # merge lat,long to zasca
    zasca_with_coords = zasca_total.merge(
        zasca_coords[["id", "gmaps_address", "latitude", "longitude"]],
        left_on="numberid_emp1",
        right_on="id",
        how="inner",
    )

    # make zasca_coords take nit instead of id
    zasca_coords = zasca_with_coords[["nit", "cohort", "gmaps_address", "latitude", "longitude"]].rename(
        columns={"nit": "id"}
    )

    # all coords to consider zasca with rues
    all_coords = pd.concat([rues_coords, zasca_coords], ignore_index=True)[
        ["id", "gmaps_address", "latitude", "longitude"]
    ]

    # merge lat,long to rues
    all_coords = all_coords.assign(nit=lambda x: x["id"].astype(str)).drop(columns=["id"])
    rues_with_coords = rues_total.merge(all_coords, on="nit", how="inner")

    # pivot rues_with_coords to have 2023 and 2024 observations (source_year col) for variables
    rues_with_coords = rues_with_coords.pivot_table(
        index=["nit", "gmaps_address", "latitude", "longitude"],
        columns="source_year",
        values=[
            "ciiu_principal",
            "cantidad_establecimientos",
            "activos_total",
            "empleados",
            "ingresos_actividad_ordinaria",
            "resultado_del_periodo",
            "cantidad_mujeres_empleadas",
            "cantidad_mujeres_en_cargos_direc",
            "codigo_tamano_empresa",
            "city",
        ],
        aggfunc="sum",
    )

    # flatten column names where multiple levels
    rues_with_coords.columns = [f"{col[0]}_{col[1]}" for col in rues_with_coords.columns if len(col) > 1]
    rues_with_coords = rues_with_coords.reset_index()
    rues_with_coords["city"] = rues_with_coords["city_2023"].fillna(rues_with_coords["city_2024"])
    rues_with_coords = rues_with_coords.drop(columns=["city_2023", "city_2024"])

    # merge with indicator as both, rename values to "only_rues", "only_zasca", "both"
    data_with_coords = rues_with_coords.merge(
        zasca_with_coords,
        on=["nit"],
        how="outer",
        suffixes=("_rues", "_zasca"),
    )

    # create unique latitude, longitude, gmaps_address, city
    data_with_coords["latitude"] = data_with_coords["latitude_rues"].fillna(data_with_coords["latitude_zasca"])
    data_with_coords["longitude"] = data_with_coords["longitude_rues"].fillna(data_with_coords["longitude_zasca"])
    data_with_coords["gmaps_address"] = data_with_coords["gmaps_address_rues"].fillna(
        data_with_coords["gmaps_address_zasca"]
    )
    data_with_coords["city"] = (
        data_with_coords["city_rues"]
        .fillna(data_with_coords["city_zasca"])
        .apply(lambda x: x.capitalize() if isinstance(x, str) else x)
    )

    # drop columns with suffixes
    data_with_coords = data_with_coords.drop(columns=[col for col in data_with_coords.columns if "_rues" in col])
    data_with_coords = data_with_coords.drop(columns=[col for col in data_with_coords.columns if "_zasca" in col])

    # drop duplicates in nit
    data_with_coords = data_with_coords.drop_duplicates(subset=["nit"])

    # save to csv
    data_with_coords.to_csv(
        DATA_DIR / "02_processed/geolocation/data_with_coords.csv", encoding="utf-8-sig", index=False
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge RUES and ZASCA data.")
    args = parser.parse_args()
    main()
