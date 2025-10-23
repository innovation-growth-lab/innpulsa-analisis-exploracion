"""Department representation analysis comparing ZASCA to EMICRON population with ISEM data."""

import logging
from pathlib import Path

import pandas as pd

from innpulsa.settings import DATA_DIR
from .utils import apply_sector_filter, MICRO_EMPRESA_THRESHOLD

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.department_representation")


@apply_sector_filter
def department_representation_analysis(
    df_zasca: pd.DataFrame, df_emicron: pd.DataFrame, df_isem: pd.DataFrame
) -> pd.DataFrame:
    """Calculate department representation ratios comparing ZASCA to EMICRON population.

    Args:
        df_zasca: ZASCA data
        df_emicron: EMICRON data
        df_isem: ISEM data with Código DANE and Puntaje ISEM

    Returns:
        pd.DataFrame: Department representation analysis with ratios and ISEM scores

    """
    df_zasca = df_zasca.copy()
    df_emicron = df_emicron.copy()
    df_isem = df_isem.copy()

    # filter out companies with more than 10 employees for zasca
    df_zasca = df_zasca.loc[df_zasca["emp_total"] < MICRO_EMPRESA_THRESHOLD]

    # calculate zasca counts by department
    zasca_counts = df_zasca["COD_DEPTO"].value_counts()

    # # calculate number of unique cohorts per department
    # zasca_cohorts = df_zasca.groupby("COD_DEPTO")["cohort"].nunique()

    # # normalize zasca counts by number of cohorts
    # zasca_normalized = zasca_counts / zasca_cohorts

    # calculate emicron population by department (using expansion factor)
    emicron_population = df_emicron.groupby("COD_DEPTO")["F_EXP"].sum()

    # start with all EMICRON departments as the base
    result_df = pd.DataFrame({
        "COD_DEPTO": emicron_population.index,
        "emicron_population": emicron_population.to_numpy(),
    })

    # add zasca data
    result_df["zasca_count"] = result_df["COD_DEPTO"].map(zasca_counts).fillna(0)  # type: ignore[reportArgumentType]
    # result_df["zasca_normalized_count"] = result_df["COD_DEPTO"].map(zasca_normalized).fillna(0)  # type: ignore[reportArgumentType]
    # result_df["num_cohorts"] = result_df["COD_DEPTO"].map(zasca_cohorts).fillna(1)  # type: ignore[reportArgumentType]

    # add ISEM data
    result_df["isem_score"] = result_df["COD_DEPTO"].map(df_isem.set_index("Código DANE")["Puntaje ISEM"])  # type: ignore[reportArgumentType]
    result_df["dept_name"] = result_df["COD_DEPTO"].map(
        df_zasca[["COD_DEPTO", "dpto"]].drop_duplicates().set_index("COD_DEPTO")["dpto"]  # type: ignore[reportArgumentType]
    ).str.title()

    # calculate proportions over shared departments only
    shared_departments = set(zasca_counts.index) & set(emicron_population.index)

    # calculate totals for shared departments only
    # total_zasca_shared = zasca_normalized[zasca_normalized.index.isin(shared_departments)].sum()
    total_zasca_shared = zasca_counts[zasca_counts.index.isin(shared_departments)].sum()
    total_emicron_shared = emicron_population[emicron_population.index.isin(shared_departments)].sum()

    # result_df["zasca_proportion_codigo"] = (result_df["zasca_normalized_count"] / total_zasca_shared * 100).fillna(0)
    result_df["zasca_proportion_codigo"] = (result_df["zasca_count"] / total_zasca_shared * 100).fillna(0)
    result_df["emicron_proportion_codigo"] = (result_df["emicron_population"] / total_emicron_shared * 100).fillna(0)

    # calculate representation ratio only for departments with ZASCA data
    result_df["representation_ratio"] = result_df.apply(
        lambda row: row["zasca_proportion_codigo"] / row["emicron_proportion_codigo"]
        if row["zasca_count"] > 0 and row["emicron_proportion_codigo"] > 0
        else None,
        axis=1,
    )

    # add status
    result_df["status"] = result_df.apply(
        lambda row: "Mayor"
        if row["representation_ratio"] is not None and row["representation_ratio"] > 1
        else "Menor"
        if row["representation_ratio"] is not None and row["representation_ratio"] > 0
        else "no_zasca_data",
        axis=1,
    )

    # drop any nan isem_score
    result_df = result_df.dropna(subset=["isem_score"])

    return result_df.sort_values("representation_ratio", ascending=False)


if __name__ == "__main__":
    SECTOR = "manufactura"
    df_zasca = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "zasca.csv", encoding="utf-8-sig")
    df_emicron = pd.read_csv(
        Path(DATA_DIR) / "01_raw" / "descriptive" / "emicron_2024_merged.csv", encoding="utf-8-sig"
    )
    df_isem = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "isem.csv", encoding="utf-8-sig")

    logger.info("creating department representation analysis for manufacturing sector")
    dept_analysis = department_representation_analysis(df_zasca, df_emicron, df_isem, filtro_por_sector=SECTOR)

    # save processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / SECTOR
    processed_dir.mkdir(parents=True, exist_ok=True)
    dept_analysis.to_csv(processed_dir / "department_representation.csv", encoding="utf-8-sig", index=False)

    logger.info("saved department representation analysis to %s", processed_dir / "department_representation.csv")
