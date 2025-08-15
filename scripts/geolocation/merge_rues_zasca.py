"""
Script to merge RUES and ZASCA data.

This script reads data from both RUES and ZASCA sources and performs an inner merge
based on business identifiers.

Usage:
    python scripts/geolocation/merge_rues_zasca.py
"""

import argparse
from pathlib import Path
import pandas as pd
import re
from math import radians, cos, sin, asin, sqrt

from innpulsa.settings import DATA_DIR
from innpulsa.logging import configure_logger

logger = configure_logger("geolocation.merge_rues_zasca")


def haversine_distance(lat1, lon1, lat2, lon2) -> float:
    """Calculate the great circle distance between two points
    on the earth (specified in decimal degrees).

    Args:
        lat1: Latitude of the first point
        lon1: Longitude of the first point
        lat2: Latitude of the second point
        lon2: Longitude of the second point

    Returns:
        float: Distance in kilometers

    """
    # convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r


def find_closest_centro(lat, lon, centros_dict):
    """
    Find the closest centro based on latitude and longitude.

    Args:
        lat: Latitude of the point
        lon: Longitude of the point
        centros_dict: Dictionary with centro names as keys and [lat, lon] as values

    Returns:
        str: Name of the closest centro

    """
    if pd.isna(lat) or pd.isna(lon):
        return None

    min_distance = float("inf")
    closest_centro = None

    for centro_name, coords in centros_dict.items():
        distance = haversine_distance(lat, lon, coords[0], coords[1])
        if distance < min_distance:
            min_distance = distance
            closest_centro = centro_name

    return closest_centro


def main() -> None:
    """
    Merge RUES and ZASCA data.

    This script will:
    - Read the RUES and ZASCA data
    - Merge the data
    - Save the results

    """
    # Define centro coordinates (same as in R script)
    centros_zasca = {
        "Bucaramanga": [7.1049364854763475, -73.12383197704348],
        "Manrique": [6.284881727521926, -75.54409932364932],
        "Medellín": [6.232088566149681, -75.56902649888393],
        "Cúcuta": [7.829409950541552, -72.46036608947021],
        "20Julio": [4.569429291819494, -74.09478949758527],
        "Baranoa": [10.803854499386958, -74.91244952786113],
        "Cali Norte": [3.4703660708293342, -76.53109251974698],
        "Cartagena": [10.408413725517383, -75.46504629117649],
        "Caucasia": [7.996741312367327, -75.19635027124215],
        "Ciudad Bolivar": [4.543213679818289, -74.1469410119057],
        "Manizales": [5.063846037654722, -75.50186555759247],
        "Riohacha": [11.539682147003058, -72.91511631324943],
        "Suba": [4.7461323779336295, -74.08267727408058],
    }

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

    # drop duplicates in "nit", "source_year"
    rues_with_coords = rues_with_coords.drop_duplicates(subset=["nit", "source_year"])

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

    # rename nit to up_id
    data_with_coords = data_with_coords.rename(columns={"nit": "up_id"})

    # remove up_id "0" and "1"
    data_with_coords = data_with_coords[~data_with_coords["up_id"].isin(["0", "1"])]

    # for control units (where centro is empty), find the closest centro
    control_mask = data_with_coords["centro"].isna()
    control_data = data_with_coords[control_mask].copy()

    if len(control_data) > 0:
        # apply the closest centro function to control units
        control_data["centro"] = control_data.apply(
            lambda row: find_closest_centro(row["latitude"], row["longitude"], centros_zasca), axis=1
        )

        # update the original dataframe with assigned centros
        data_with_coords.loc[control_mask, "centro"] = control_data["centro"]

    # create a mapping of centro to lowest yearcohort from treated observations
    centro_yearcohort_mapping = {}
    for centro in data_with_coords["centro"].dropna().unique():
        centro_data = data_with_coords[data_with_coords["centro"] == centro]
        # get the lowest yearcohort for this centro (only from treated observations)
        yearcohorts = centro_data["yearcohort"].dropna()
        lowest_yearcohort = yearcohorts.min()
        centro_yearcohort_mapping[centro] = lowest_yearcohort
        logger.info("  %s: yearcohort %s", centro, lowest_yearcohort)

    # apply the mapping only to control observations (where yearcohort is NaN)
    control_yearcohort_mask = data_with_coords["yearcohort"].isna()
    data_with_coords.loc[control_yearcohort_mask, "yearcohort"] = data_with_coords.loc[
        control_yearcohort_mask, "centro"
    ].map(centro_yearcohort_mapping)

    # save to csv
    data_with_coords.to_csv(
        DATA_DIR / "02_processed/geolocation/data_with_coords.csv", encoding="utf-8-sig", index=False
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge RUES and ZASCA data.")
    args = parser.parse_args()
    main()
