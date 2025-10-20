import logging
from pathlib import Path

import pandas as pd

from innpulsa.settings import DATA_DIR
from data_processing.age_distribution import diferencias_de_edad
from data_processing.gender_distribution import diferencias_de_genero
from data_processing.household_head import porcentaje_jefa_hogar
from data_processing.sisben_groups import proporciones_grupos_sisben
from data_processing.household_care import household_care_data
from data_processing.department_representation import department_representation_analysis
from data_processing.business_age import business_age_analysis
from data_processing.sales import sales
from data_processing.employment import employment
from data_processing.reasons import reasons

logger = logging.getLogger("innpulsa.scripts.descriptive.create_plot_data")


def save_processed_data(data: pd.DataFrame, filename: str, sector: str) -> None:
    """Save processed data to csv file.

    Args:
        data: processed dataframe to save
        filename: name of the output file
        sector: sector name for directory structure

    """
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / sector
    processed_dir.mkdir(parents=True, exist_ok=True)
    data.to_csv(processed_dir / filename, encoding="utf-8-sig", index=False)
    logger.info("saved %s data to %s", filename.replace(".csv", ""), processed_dir / filename)


if __name__ == "__main__":
    SECTOR = "manufactura"
    df_zasca = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "zasca.csv", encoding="utf-8-sig")
    df_emicron_2024_merged = pd.read_csv(
        Path(DATA_DIR) / "01_raw" / "descriptive" / "emicron_2024_merged.csv", encoding="utf-8-sig"
    )
    df_personal_ocupado = pd.read_csv(
        Path(DATA_DIR) / "01_raw" / "descriptive" / "personal_ocupado.csv", encoding="utf-8-sig"
    )
    df_sisben = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "sisben.csv", encoding="utf-8-sig")
    df_isem = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "isem.csv", encoding="utf-8-sig")
    df_rues = pd.read_csv(Path(DATA_DIR) / "01_raw" / "descriptive" / "rues.csv", encoding="utf-8-sig")

    logger.info("creating age distribution data for manufacturing sector")
    age_data = diferencias_de_edad(df_zasca, df_emicron_2024_merged, filtro_por_sector=SECTOR)
    save_processed_data(age_data, "age_distribution.csv", SECTOR)

    logger.info("creating gender distribution data for manufacturing sector")
    gender_data = diferencias_de_genero(df_zasca, df_personal_ocupado, filtro_por_sector=SECTOR)
    save_processed_data(gender_data, "gender_distribution.csv", SECTOR)

    logger.info("creating household head data for manufacturing sector")
    household_data = porcentaje_jefa_hogar(df_zasca, filtro_por_sector=SECTOR)
    save_processed_data(household_data, "household_head.csv", SECTOR)

    logger.info("creating sisben group data for manufacturing sector")
    sisben_data = proporciones_grupos_sisben(df_zasca, df_sisben, filtro_por_sector=SECTOR)
    save_processed_data(sisben_data, "sisben_groups.csv", SECTOR)

    logger.info("creating household care data for manufacturing sector")
    household_care_data = household_care_data(df_zasca, filtro_por_sector=SECTOR)
    save_processed_data(household_care_data, "household_care.csv", SECTOR)

    logger.info("creating department representation analysis for manufacturing sector")
    dept_analysis = department_representation_analysis(
        df_zasca, df_emicron_2024_merged, df_isem, filtro_por_sector=SECTOR
    )
    save_processed_data(dept_analysis, "department_representation.csv", SECTOR)

    logger.info("creating business age analysis for manufacturing sector")
    business_age_data = business_age_analysis(df_zasca, df_emicron_2024_merged, filtro_por_sector=SECTOR)
    save_processed_data(business_age_data, "business_age.csv", SECTOR)

    logger.info("creating sales analysis for manufacturing sector")
    sales_data = sales(df_zasca, df_emicron_2024_merged, df_rues, filtro_por_sector=SECTOR)
    save_processed_data(sales_data, "sales.csv", SECTOR)

    logger.info("creating employment analysis for manufacturing sector")
    employment_data = employment(df_zasca, df_personal_ocupado, df_rues, filtro_por_sector=SECTOR)
    save_processed_data(employment_data, "employment.csv", SECTOR)

    logger.info("creating reasons-for-entrepreneurship data for manufacturing sector")
    reasons_data = reasons(df_zasca, df_emicron_2024_merged, filtro_por_sector=SECTOR)
    save_processed_data(reasons_data, "reasons.csv", SECTOR)
