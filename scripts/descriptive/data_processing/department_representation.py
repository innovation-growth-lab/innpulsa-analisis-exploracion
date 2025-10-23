"""Department representation analysis comparing ZASCA to EMICRON population with ISEM data."""

import logging
from pathlib import Path

import pandas as pd

from innpulsa.settings import DATA_DIR
from .utils import MICRO_EMPRESA_THRESHOLD

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.department_representation")

# Minimum number of beneficiaries required for a department to be included in agro sector analysis
MIN_BENEFICIARIES_AGRO = 5


def department_representation_analysis(df_zasca: pd.DataFrame, df_isem: pd.DataFrame) -> pd.DataFrame:
    """Calculate department ZASCA counts vs ISEM scores by sector.

    Args:
        df_zasca: ZASCA data (should include both agro and manufacturing sectors)
        df_isem: ISEM data with Código DANE and Puntaje ISEM

    Returns:
        pd.DataFrame: Department ZASCA counts with ISEM scores by sector

    """
    df_zasca = df_zasca.copy()
    df_isem = df_isem.copy()

    # filter out companies with more than 10 employees for zasca
    df_zasca = df_zasca.loc[df_zasca["emp_total"] < MICRO_EMPRESA_THRESHOLD]

    # process each sector separately
    results = []

    for sector_code, sector_name in [(3, "manufactura"), (1, "agro")]:
        df_sector = df_zasca[df_zasca["GRUPOS12"] == sector_code].copy()

        # calculate zasca counts by department for this sector
        zasca_counts = df_sector["COD_DEPTO"].value_counts()

        # For agro sector, filter out departments with fewer than MIN_BENEFICIARIES_AGRO beneficiaries
        if sector_name == "agro":
            departments_to_keep: pd.Index = zasca_counts[zasca_counts >= MIN_BENEFICIARIES_AGRO].index
            df_sector = df_sector.loc[df_sector["COD_DEPTO"].isin(departments_to_keep.tolist())]
            zasca_counts = df_sector["COD_DEPTO"].value_counts()

        # create result dataframe for this sector
        sector_df = pd.DataFrame({
            "COD_DEPTO": zasca_counts.index,
            "zasca_count": zasca_counts.to_list(),
            "sector": sector_name,
        })

        # add ISEM data
        sector_df["isem_score"] = sector_df["COD_DEPTO"].map(df_isem.set_index("Código DANE")["Puntaje ISEM"])  # type: ignore[reportArgumentType]
        sector_df["dept_name"] = (
            sector_df["COD_DEPTO"]
            .map(
                df_sector[["COD_DEPTO", "dpto"]].drop_duplicates().set_index("COD_DEPTO")["dpto"]  # type: ignore[reportArgumentType]
            )
            .str.title()
        )

        # drop any nan isem_score
        sector_df = sector_df.dropna(subset=["isem_score"])

        results.append(sector_df)

    # combine both sectors
    result_df = pd.concat(results, ignore_index=True)

    return result_df.sort_values(["sector", "zasca_count"], ascending=[True, False])


if __name__ == "__main__":
    df_zasca = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "zasca.csv", encoding="utf-8-sig")
    df_isem = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "isem.csv", encoding="utf-8-sig")

    logger.info("creating department representation analysis for both sectors")
    dept_analysis = department_representation_analysis(df_zasca, df_isem)

    # save processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / "all"
    processed_dir.mkdir(parents=True, exist_ok=True)
    dept_analysis.to_csv(processed_dir / "department_representation.csv", encoding="utf-8-sig", index=False)

    logger.info("saved department representation analysis to %s", processed_dir / "department_representation.csv")
