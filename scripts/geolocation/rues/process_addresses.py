#!/usr/bin/env python3
"""Process RUES commercial addresses via Gemini LLM to standardise street names.

python scripts/geolocation/rues/process_addresses.py --prompt rues
"""

import asyncio
import os
import sys
import json
from pathlib import Path
from typing import Optional
import argparse

import pandas as pd

from innpulsa.processing.rues import read_processed_rues
from innpulsa.loaders import load_csv
from innpulsa.geolocation.llm import normalise_addresses_using_llm
from innpulsa.geolocation.prompts import (
    SYSTEM_PROMPT_ZASCA,
    SYSTEM_PROMPT_RUES,
)
from innpulsa.settings import DATA_DIR
from innpulsa.logging import configure_logger

logger = configure_logger("geolocation.rues_llm")

OUTPUT_DIR = Path(DATA_DIR) / "processed/geolocation/rues_addresses"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def filter_rues_against_zasca(rues: pd.DataFrame, zasca: pd.DataFrame) -> pd.DataFrame:
    """Filter RUES data to only include rows whose city appears in ZASCA.
    It also samples 500 rows from each city to reduce the number of addresses
    processed by the LLM.

    Args:
        rues: DataFrame containing RUES records with city and state columns
        zasca: DataFrame containing ZASCA records with city and state columns

    Returns:
        DataFrame containing filtered RUES records that have cities present in ZASCA
    """

    # [AD-HOC] ZASCA fixes
    zasca.loc[zasca["city"] == "Donmatías", "city"] = "Don Matías"

    # In the future we'll need to add Bogota (a state in RUES nomenclature)

    valid_cities = (
        zasca["city"]
        .dropna()
        .str.lower()
        .str.strip()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .drop_duplicates()
    )

    rues = rues.copy()
    rues["city_norm"] = (
        rues["city"]
        .str.lower()
        .str.strip()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    filtered = rues[rues["city_norm"].isin(valid_cities)]

    # sample from each city
    filtered = (
        filtered.groupby("city_norm")
        .apply(lambda x: x.sample(n=min(len(x), 1000), random_state=42))
        .reset_index(drop=True)
    )

    logger.info(
        "filtered RUES from %d ➜ %d rows with matching city",
        len(rues),
        len(filtered),
    )
    return filtered.drop(columns=["city_norm"])


def build_address(df: pd.DataFrame) -> pd.DataFrame:
    """Create full_address and id columns expected by LLM helpers."""
    df = df.copy()
    df["full_address"] = (
        df["dirección_comercial"].fillna("")
        + ", "
        + df["city"].fillna("")
        + ", "
        + df["state"].fillna("")
        + ", CO"
    )

    # LLM helper expects identifier column named numberid_emp1 – create alias
    df["numberid_emp1"] = df["nit"]
    return df


async def run_pipeline(prompt: str) -> int:
    """Run the pipeline."""
    if not os.getenv("GEMINI_API_KEY"):
        logger.error("GEMINI_API_KEY environment variable is required")
        return 1

    rues_df = read_processed_rues()
    zasca_geocoded = load_csv(
        Path(DATA_DIR) / "processed/geolocation/zasca_addresses.csv",
        encoding="latin1",
    )

    # select only rues_df observations in source_year 2023
    rues_df = rues_df[rues_df["source_year"] == 2023]

    rues_filtered = filter_rues_against_zasca(rues_df, zasca_geocoded)
    rues_prepared = build_address(rues_filtered)

    # Run processing (reuse ZASCA pipeline)
    await normalise_addresses_using_llm(
        rues_prepared,
        OUTPUT_DIR,
        prompt=prompt,
    )

    # Compile results
    compiled = compile_results()
    if compiled is None:
        logger.error("No results to compile")
        return 1

    out_csv = Path(DATA_DIR) / "processed/geolocation/rues_addresses.csv"
    compiled.to_csv(out_csv, index=False, encoding="utf-8-sig")
    logger.info("Saved %d standardised addresses to %s", len(compiled), out_csv)
    return 0


def compile_results() -> Optional[pd.DataFrame]:
    """Gather all *success.json files in OUTPUT_DIR and return DataFrame."""
    success_files = list(OUTPUT_DIR.glob("batch_*_success.json"))
    if not success_files:
        return None

    records = []
    for path in success_files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                batch = json.load(f)
            response = json.loads(batch["response"])
            for id_, result in response.items():
                records.append(
                    {
                        "nit": id_,
                        "raw_address": batch["input_addresses"].get(id_, ""),
                        "formatted_address": result.get("formatted_address"),
                        "country": result.get("country"),
                        "area": result.get("area"),
                        "city": result.get("city"),
                    }
                )
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("error parsing %s: %s", path, exc)

    return pd.DataFrame(records) if records else None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process RUES addresses with Gemini LLM and standardise street names",
    )
    parser.add_argument(
        "--prompt",
        choices=[
            "rues"
        ],  # ["zasca"] Keep argparse in case we merge process address scripts.
        default="rues",
        help="Which prompt template to use (default: rues)",
    )

    args = parser.parse_args()

    SELECTED_PROMPTS = (
        SYSTEM_PROMPT_RUES if args.prompt == "rues" else SYSTEM_PROMPT_ZASCA
    )

    sys.exit(asyncio.run(run_pipeline(SELECTED_PROMPTS)))
