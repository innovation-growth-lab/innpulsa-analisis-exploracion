"""Main script for creating plots."""

import logging
from pathlib import Path

import pandas as pd

from innpulsa.settings import DATA_DIR
from plots.mirror_histogram import plot_mirror_histogram_with_excess
from plots.marimekko_chart import plot_marimekko_gender_comparison
from plots.sisben_groups import plot_sisben_groups_diverging
from plots.household_care import plot_household_care_violin
from plots.department_representation import plot_department_representation_scatter
from plots.business_age import plot_business_age_raincloud, plot_business_age_raincloud_emicron
from plots.sales import plot_sales_raincloud_combined
from plots.employment import plot_employment_dumbbell_by_category

logger = logging.getLogger("innpulsa.scripts.descriptive.plot_mirror_histogram")


if __name__ == "__main__":
    SECTOR = "manufactura"
    # load processed data
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / SECTOR
    df_plot = pd.read_csv(processed_dir / "age_distribution.csv", encoding="utf-8-sig")

    # create and save plot
    chart = plot_mirror_histogram_with_excess(df_plot)

    # save as png
    output_dir = Path(DATA_DIR) / "03_outputs" / "descriptive" / SECTOR
    output_dir.mkdir(parents=True, exist_ok=True)
    chart.save(str(output_dir / "age_distribution_mirror_histogram.png"), scale_factor=2.0, ppi=300)

    logger.info("saved mirror histogram to %s", output_dir / "age_distribution_mirror_histogram.png")

    # create and save Marimekko chart for gender distribution
    logger.info("creating Marimekko chart for gender distribution")
    df_gender = pd.read_csv(processed_dir / "gender_distribution.csv", encoding="utf-8-sig")
    marimekko_chart = plot_marimekko_gender_comparison(df_gender)

    # save Marimekko chart as png
    marimekko_chart.save(str(output_dir / "gender_distribution_marimekko.png"), scale_factor=2.0, ppi=300)

    logger.info("saved Marimekko chart to %s", output_dir / "gender_distribution_marimekko.png")

    # create and save Sisbén groups diverging chart
    logger.info("creating Sisbén groups diverging chart")
    df_sisben = pd.read_csv(processed_dir / "sisben_groups.csv", encoding="utf-8-sig")
    sisben_chart = plot_sisben_groups_diverging(df_sisben)

    # save Sisbén groups chart as png
    sisben_chart.save(str(output_dir / "sisben_groups_diverging.png"), scale_factor=2.0, ppi=300)

    logger.info("saved Sisbén groups chart to %s", output_dir / "sisben_groups_diverging.png")

    # create and save household care violin plot
    logger.info("creating household care violin plot")
    df_household_care = pd.read_csv(processed_dir / "household_care.csv", encoding="utf-8-sig")
    household_care_chart = plot_household_care_violin(df_household_care)

    # save household care chart as png
    household_care_chart.save(str(output_dir / "household_care_violin.png"), scale_factor=2.0, ppi=300)

    logger.info("saved household care violin plot to %s", output_dir / "household_care_violin.png")

    # create and save department representation scatter plot
    logger.info("creating department representation scatter plot")
    df_dept = pd.read_csv(processed_dir / "department_representation.csv", encoding="utf-8-sig")
    dept_chart = plot_department_representation_scatter(df_dept)

    # save department representation chart as png
    dept_chart.save(str(output_dir / "department_representation_scatter.png"), scale_factor=2.0, ppi=300)

    logger.info(
        "saved department representation scatter plot to %s", output_dir / "department_representation_scatter.png"
    )

    # create and save business age raincloud plots
    logger.info("creating business age raincloud plots")
    df_business_age = pd.read_csv(processed_dir / "business_age.csv", encoding="utf-8-sig")

    # create ZASCA raincloud plot
    business_age_raincloud_zasca = plot_business_age_raincloud(df_business_age)
    business_age_raincloud_zasca.save(str(output_dir / "business_age_raincloud_zasca.png"), scale_factor=2.0, ppi=300)
    logger.info("saved ZASCA business age raincloud plot to %s", output_dir / "business_age_raincloud_zasca.png")

    # create EMICRON raincloud plot
    business_age_raincloud_emicron = plot_business_age_raincloud_emicron(df_business_age)
    business_age_raincloud_emicron.save(
        str(output_dir / "business_age_raincloud_emicron.png"), scale_factor=2.0, ppi=300
    )
    logger.info("saved EMICRON business age raincloud plot to %s", output_dir / "business_age_raincloud_emicron.png")

    # create and save sales raincloud plots
    logger.info("creating sales raincloud plots")
    df_sales = pd.read_csv(processed_dir / "sales.csv", encoding="utf-8-sig")

    # create combined sales raincloud plot
    sales_raincloud_combined = plot_sales_raincloud_combined(df_sales)
    sales_raincloud_combined.save(str(output_dir / "sales_raincloud.png"), scale_factor=2.0, ppi=300)
    logger.info("saved combined sales raincloud plot to %s", output_dir / "sales_raincloud.png")

    # create and save employment dumbbell plots
    logger.info("creating employment dumbbell plots")
    df_employment = pd.read_csv(processed_dir / "employment.csv", encoding="utf-8-sig")

    # create employment dumbbell plots by category
    employment_dumbbell_by_category = plot_employment_dumbbell_by_category(df_employment)
    employment_dumbbell_by_category.save(
        str(output_dir / "employment_dumbbell.png"), scale_factor=2.0, ppi=300
    )
    logger.info("saved employment dumbbell by category plot to %s", output_dir / "employment_dumbbell_by_category.png")
