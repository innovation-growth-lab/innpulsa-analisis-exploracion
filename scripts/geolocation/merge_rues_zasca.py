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
from operator import itemgetter

from innpulsa.settings import DATA_DIR
from innpulsa.logging import configure_logger

logger = configure_logger("geolocation.merge_rues_zasca")

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


def find_closest_centro_with_ciiu_match(location, centros_dict, ciiu_principal, centro_ciiu_mapping, row_index=None):
    """
    Find the closest centro that matches one of the top three CIIU codes from ZASCA participants for that center.

    Args:
        location: Tuple of (latitude, longitude)
        centros_dict: Dictionary with centro names as keys and [lat, lon] as values
        ciiu_principal: CIIU code of the current business
        centro_ciiu_mapping: Dictionary mapping centro names to their top 3 CIIU codes
        row_index: Index of the current row (used for deterministic assignment between Manrique/Medellín)

    Returns:
        str: Name of the closest centro that matches CIIU, or None if no match found

    """
    lat, lon = location
    # convert ciiu_principal to string for comparison
    ciiu_str = str(ciiu_principal) if not pd.isna(ciiu_principal) else None

    # find the closest centro that has matching CIIU codes
    min_distance = float("inf")
    closest_centro = None
    matching_centros = []

    for centro_name, coords in centros_dict.items():
        # get top CIIU codes for this center
        top_ciiu_codes = centro_ciiu_mapping.get(centro_name, [])

        # if no CIIU codes available for this center, skip it
        if not top_ciiu_codes:
            continue

        # check if current business CIIU matches any of the top codes for this center
        top_ciiu_strs = [str(code) for code in top_ciiu_codes]
        if ciiu_str in top_ciiu_strs:
            # CIIU matches, calculate distance
            distance = haversine_distance(lat, lon, coords[0], coords[1])
            matching_centros.append((centro_name, distance))

            if distance < min_distance:
                min_distance = distance
                closest_centro = centro_name

    # Special handling for Manrique and Medellín (same city)
    if closest_centro in {"Manrique", "Medellín"}:
        # Check if both Manrique and Medellín are among the matching centros
        manrique_medellin_matches = [
            (centro, dist) for centro, dist in matching_centros if centro in {"Manrique", "Medellín"}
        ]

        if len(manrique_medellin_matches) >= 2:  # noqa: PLR2004
            # Both are available, assign based on row index for roughly equal distribution
            if row_index is not None:
                if row_index % 2 == 0:
                    return "Manrique"
                return "Medellín"
            # fallback: use the closest one
            manrique_medellin_matches.sort(key=itemgetter(1))
            return manrique_medellin_matches[0][0]

    return closest_centro


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


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load all required data files.

    Returns:
        tuple: (rues_total, rues_coords, zasca_total, zasca_coords)

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

    return rues_total, rues_coords, zasca_total, zasca_coords


def clean_zasca_nit(zasca_total: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize NIT values in ZASCA data.

    Args:
        zasca_total: Raw ZASCA DataFrame

    Returns:
        DataFrame with cleaned NIT values

    """
    zasca_clean = zasca_total.copy()

    # let's sort of assume that if nit is missing in zasca_total, it may be numberid_emp1
    zasca_clean["nit"] = zasca_clean["nit"].fillna(zasca_clean["numberid_emp1"])

    # if str(nit) with last character removed matches numberid_emp1, then replace nit with numberid_emp1
    zasca_clean["nit"] = zasca_clean.apply(
        lambda row: row["numberid_emp1"] if str(row["nit"])[:-1] == str(row["numberid_emp1"]) else row["nit"], axis=1
    )

    # also remap specific nits
    zasca_clean["nit"] = zasca_clean["nit"].replace({
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
    zasca_clean["nit"] = zasca_clean["nit"].apply(lambda x: x if pd.isna(x) else re.sub(r"([ -])\d", "", str(x)))

    return zasca_clean


def prepare_coordinates_data(
    rues_total: pd.DataFrame, rues_coords: pd.DataFrame, zasca_total: pd.DataFrame, zasca_coords: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare and merge coordinate data from both sources.

    Args:
        rues_total: RUES total data
        rues_coords: RUES coordinates data
        zasca_total: ZASCA total data
        zasca_coords: ZASCA coordinates data

    Returns:
        DataFrame with merged coordinate data

    """
    # merge lat,long to zasca
    zasca_with_coords = zasca_total.merge(
        zasca_coords[["id", "gmaps_address", "latitude", "longitude"]],
        left_on="numberid_emp1",
        right_on="id",
        how="inner",
    )

    # make zasca_coords take nit instead of id
    zasca_coords_clean = zasca_with_coords[["nit", "cohort", "gmaps_address", "latitude", "longitude"]].rename(
        columns={"nit": "id"}
    )

    # all coords to consider zasca with rues
    all_coords = pd.concat([rues_coords, zasca_coords_clean], ignore_index=True)[
        ["id", "gmaps_address", "latitude", "longitude"]
    ]

    # merge lat,long to rues
    all_coords = all_coords.assign(nit=lambda x: x["id"].astype(str)).drop(columns=["id"])
    rues_with_coords = rues_total.merge(all_coords, on="nit", how="inner")

    return rues_with_coords, zasca_with_coords


def pivot_rues_data(rues_with_coords: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot RUES data to have 2023 and 2024 observations.

    Args:
        rues_with_coords: RUES data with coordinates

    Returns:
        DataFrame with pivoted RUES data

    """
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
    return rues_with_coords.drop(columns=["city_2023", "city_2024"])


def merge_datasets(rues_with_coords: pd.DataFrame, zasca_with_coords: pd.DataFrame) -> pd.DataFrame:
    """
    Merge RUES and ZASCA datasets.

    Args:
        rues_with_coords: Pivoted RUES data
        zasca_with_coords: ZASCA data with coordinates

    Returns:
        Merged DataFrame

    """
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

    # remove up_id "0" and "1" and return
    return data_with_coords[~data_with_coords["up_id"].isin(["0", "1"])]  # type: ignore[return-value]


def get_centro_ciiu_mapping(data_with_coords: pd.DataFrame) -> dict:
    """
    Get mapping of centers to their top 3 CIIU codes from ZASCA participants.

    Args:
        data_with_coords: Merged dataset

    Returns:
        Dictionary mapping centro names to top 3 CIIU codes

    """
    # get top 3 CIIU codes per center from ZASCA participants (rows with sales2022s not being NaN)
    zasca_participants = data_with_coords[data_with_coords["sales2022s"].notna()].copy()

    # create a mapping of center to top 3 CIIU codes from ZASCA participants
    centro_ciiu_mapping = {}
    for centro in centros_zasca:
        # get ZASCA participants for this center
        centro_participants = zasca_participants[zasca_participants["centro"] == centro]

        # get top 3 CIIU codes for this center
        ciiu_counts = centro_participants["ciiu_principal_2023"].value_counts()
        top_3_ciiu_codes = ciiu_counts.head(3).index.tolist()
        centro_ciiu_mapping[centro] = top_3_ciiu_codes

        logger.info("  %s: top 3 CIIU codes %s", centro, top_3_ciiu_codes)

    return centro_ciiu_mapping


def assign_control_centros(data_with_coords: pd.DataFrame, centro_ciiu_mapping: dict) -> pd.DataFrame:
    """
    Assign centers to control units based on CIIU matching and distance.

    Args:
        data_with_coords: Dataset with control units
        centro_ciiu_mapping: Mapping of centers to CIIU codes

    Returns:
        DataFrame with assigned centers

    """
    # for control units (where centro is empty), find the closest centro with CIIU matching
    control_mask = data_with_coords["centro"].isna()
    control_data = data_with_coords[control_mask].copy()

    # apply the closest centro function with CIIU matching to control units
    control_data["centro"] = control_data.apply(
        lambda row: find_closest_centro_with_ciiu_match(
            (row["latitude"], row["longitude"]),
            centros_zasca,
            row["ciiu_principal_2023"],
            centro_ciiu_mapping,
            row.name,
        ),
        axis=1,
    )

    # update the original dataframe with assigned centros
    data_with_coords.loc[control_mask, "centro"] = control_data["centro"]

    return data_with_coords


def assign_yearcohorts(data_with_coords: pd.DataFrame) -> pd.DataFrame:
    """
    Assign yearcohorts to control units based on their assigned centers.

    Args:
        data_with_coords: Dataset with assigned centers

    Returns:
        DataFrame with assigned yearcohorts

    """
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

    return data_with_coords


def main() -> None:
    """
    Merge RUES and ZASCA data.

    This script will:
    - Read the RUES and ZASCA data
    - Merge the data
    - Save the results
    """
    logger.info("Starting RUES and ZASCA data merge process")

    # Load data
    rues_total, rues_coords, zasca_total, zasca_coords = load_data()
    logger.info("Data loaded successfully")

    # Clean ZASCA NIT values
    zasca_total = clean_zasca_nit(zasca_total)
    logger.info("ZASCA NIT values cleaned")

    # Prepare coordinate data
    rues_with_coords, zasca_with_coords = prepare_coordinates_data(rues_total, rues_coords, zasca_total, zasca_coords)
    logger.info("Coordinate data prepared")

    # Pivot RUES data
    rues_with_coords = pivot_rues_data(rues_with_coords)
    logger.info("RUES data pivoted")

    # Merge datasets
    data_with_coords = merge_datasets(rues_with_coords, zasca_with_coords)
    logger.info("Datasets merged")

    # Get centro-CIIU mapping
    centro_ciiu_mapping = get_centro_ciiu_mapping(data_with_coords)
    logger.info("Centro-CIIU mapping created")

    # Assign centers to control units
    data_with_coords = assign_control_centros(data_with_coords, centro_ciiu_mapping)
    logger.info("Control centers assigned")

    # Assign yearcohorts
    data_with_coords = assign_yearcohorts(data_with_coords)
    logger.info("Yearcohorts assigned")

    # Save results
    data_with_coords.to_csv(
        DATA_DIR / "02_processed/geolocation/data_with_coords.csv", encoding="utf-8-sig", index=False
    )
    logger.info("Results saved to data_with_coords.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge RUES and ZASCA data.")
    args = parser.parse_args()
    main()
