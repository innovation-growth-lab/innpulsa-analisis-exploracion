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
from plots.business_age import (
    plot_business_age_raincloud,
    plot_business_age_raincloud_emicron,
    plot_sector_experience_raincloud,
    plot_total_experience_raincloud,
)
from plots.sales import plot_sales_raincloud_combined
from plots.employment import plot_employment_dumbbell_by_category, plot_employment_dumbbell_by_category_combined
from plots.reasons import plot_reasons_butterfly, plot_reasons_butterfly_combined
from plots.formality import plot_formality_by_indicator, plot_formality_by_indicator_combined

logger = logging.getLogger("innpulsa.scripts.descriptive.create_plots")


if __name__ == "__main__":
    SECTOR = "manufactura"

    # Setup all directories
    processed_dir = Path(DATA_DIR) / "02_processed" / "descriptive" / SECTOR
    output_dir = Path(DATA_DIR) / "03_outputs" / "descriptive" / SECTOR
    output_dir.mkdir(parents=True, exist_ok=True)

    processed_dir_agro = Path(DATA_DIR) / "02_processed" / "descriptive" / "agro"
    output_dir_agro = Path(DATA_DIR) / "03_outputs" / "descriptive" / "agro"
    output_dir_agro.mkdir(parents=True, exist_ok=True)

    processed_dir_all = Path(DATA_DIR) / "02_processed" / "descriptive" / "all"
    output_dir_all = Path(DATA_DIR) / "03_outputs" / "descriptive" / "all"
    output_dir_all.mkdir(parents=True, exist_ok=True)

    # load processed data
    df_plot = pd.read_csv(processed_dir / "age_distribution.csv", encoding="utf-8-sig")

    # create and save plot
    chart = plot_mirror_histogram_with_excess(df_plot)

    # save as png
    chart.save(str(output_dir / "age_distribution_mirror_histogram.png"), scale_factor=3.0, ppi=400)

    logger.info("saved mirror histogram to %s", output_dir / "age_distribution_mirror_histogram.png")

    # save age distribution for both sectors
    logger.info("saving age distribution for both sectors")
    df_age_all = pd.read_csv(processed_dir_all / "age_distribution.csv", encoding="utf-8-sig")
    age_chart_all = plot_mirror_histogram_with_excess(df_age_all)
    age_chart_all.save(str(output_dir_all / "age_distribution_mirror_histogram.png"), scale_factor=3.0, ppi=400)
    logger.info(
        "saved age distribution for both sectors to %s", output_dir_all / "age_distribution_mirror_histogram.png"
    )

    # create and save Marimekko chart for gender distribution - agro
    logger.info("creating Marimekko chart for gender distribution")
    df_gender_agro = pd.read_csv(processed_dir_agro / "gender_distribution.csv", encoding="utf-8-sig")
    marimekko_chart_agro = plot_marimekko_gender_comparison(df_gender_agro)

    # save Marimekko chart as png
    marimekko_chart_agro.save(str(output_dir_agro / "gender_distribution_marimekko.png"), scale_factor=3.0, ppi=400)

    logger.info("saved Marimekko chart to %s", output_dir_agro / "gender_distribution_marimekko.png")

    # create and save Marimekko chart for gender distribution - manufactura
    df_gender = pd.read_csv(processed_dir / "gender_distribution.csv", encoding="utf-8-sig")
    marimekko_chart = plot_marimekko_gender_comparison(df_gender)

    # save Marimekko chart as png
    marimekko_chart.save(str(output_dir / "gender_distribution_marimekko.png"), scale_factor=3.0, ppi=400)

    logger.info("saved Marimekko chart to %s", output_dir / "gender_distribution_marimekko.png")

    # create and save Sisbén groups diverging chart
    logger.info("creating Sisbén groups diverging chart")
    df_sisben = pd.read_csv(processed_dir / "sisben_groups.csv", encoding="utf-8-sig")
    sisben_chart = plot_sisben_groups_diverging(df_sisben)

    # save Sisbén groups chart as png
    sisben_chart.save(str(output_dir / "sisben_groups_diverging.png"), scale_factor=3.0, ppi=400)

    logger.info("saved Sisbén groups chart to %s", output_dir / "sisben_groups_diverging.png")

    # create and save household care violin plot
    logger.info("creating household care violin plot")
    df_household_care = pd.read_csv(processed_dir / "household_care.csv", encoding="utf-8-sig")
    household_care_chart = plot_household_care_violin(df_household_care)

    # save household care chart as png
    household_care_chart.save(str(output_dir / "household_care_violin.png"), scale_factor=3.0, ppi=400)

    logger.info("saved household care violin plot to %s", output_dir / "household_care_violin.png")

    # create and save department representation scatter plot - both sectors side by side
    logger.info("creating department representation scatter plot for both sectors")
    df_dept_all = pd.read_csv(processed_dir_all / "department_representation.csv", encoding="utf-8-sig")
    dept_chart_all = plot_department_representation_scatter(df_dept_all)
    dept_chart_all.save(str(output_dir_all / "department_representation_scatter.png"), scale_factor=3.0, ppi=400)
    logger.info(
        "saved department representation scatter plot to %s", output_dir_all / "department_representation_scatter.png"
    )

    # create and save business age raincloud plots
    logger.info("creating business age raincloud plots")
    df_business_age = pd.read_csv(processed_dir / "business_age.csv", encoding="utf-8-sig")

    # create ZASCA raincloud plot
    business_age_raincloud_zasca = plot_business_age_raincloud(df_business_age)
    business_age_raincloud_zasca.save(str(output_dir / "business_age_raincloud_zasca.png"), scale_factor=3.0, ppi=400)
    logger.info("saved ZASCA business age raincloud plot to %s", output_dir / "business_age_raincloud_zasca.png")

    # create EMICRON raincloud plot
    business_age_raincloud_emicron = plot_business_age_raincloud_emicron(df_business_age)
    business_age_raincloud_emicron.save(
        str(output_dir / "business_age_raincloud_emicron.png"), scale_factor=3.0, ppi=400
    )
    logger.info("saved EMICRON business age raincloud plot to %s", output_dir / "business_age_raincloud_emicron.png")

    # create sector experience raincloud plot
    sector_experience_raincloud = plot_sector_experience_raincloud(df_business_age)
    sector_experience_raincloud.save(str(output_dir / "sector_experience_raincloud.png"), scale_factor=3.0, ppi=400)
    logger.info("saved sector experience raincloud plot to %s", output_dir / "sector_experience_raincloud.png")

    # create total experience raincloud plot
    total_experience_raincloud = plot_total_experience_raincloud(df_business_age)
    total_experience_raincloud.save(str(output_dir / "total_experience_raincloud.png"), scale_factor=3.0, ppi=400)
    logger.info("saved total experience raincloud plot to %s", output_dir / "total_experience_raincloud.png")

    # create and save sales raincloud plots
    logger.info("creating sales raincloud plots")
    df_sales = pd.read_csv(processed_dir / "sales.csv", encoding="utf-8-sig")

    # create combined sales raincloud plot
    sales_raincloud_combined = plot_sales_raincloud_combined(df_sales)
    sales_raincloud_combined.save(str(output_dir / "sales_raincloud.png"), scale_factor=3.0, ppi=400)
    logger.info("saved combined sales raincloud plot to %s", output_dir / "sales_raincloud.png")

    # create and save employment dumbbell plots
    logger.info("creating employment dumbbell plots")
    df_employment = pd.read_csv(processed_dir / "employment.csv", encoding="utf-8-sig")

    # create employment dumbbell plots by category
    employment_dumbbell_by_category = plot_employment_dumbbell_by_category(df_employment)
    employment_dumbbell_by_category.save(str(output_dir / "employment_dumbbell.png"), scale_factor=3.0, ppi=400)
    logger.info("saved employment dumbbell by category plot to %s", output_dir / "employment_dumbbell_by_category.png")

    # reasons-for-entrepreneurship butterfly plot
    logger.info("creating reasons-for-entrepreneurship butterfly plot")
    df_reasons = pd.read_csv(processed_dir / "reasons.csv", encoding="utf-8-sig")
    reasons_chart = plot_reasons_butterfly(df_reasons)
    reasons_chart.save(str(output_dir / "reasons_butterfly.png"), scale_factor=3.0, ppi=400)
    logger.info("saved reasons butterfly plot to %s", output_dir / "reasons_butterfly.png")

    # formality indicators butterfly plots
    logger.info("creating formality indicators butterfly plots")
    df_formality = pd.read_csv(processed_dir / "formality.csv", encoding="utf-8-sig")
    formality_butterfly = plot_formality_by_indicator(df_formality)
    formality_butterfly.save(str(output_dir / "formality_butterfly.png"), scale_factor=3.0, ppi=400)
    logger.info("saved formality butterfly plot to %s", output_dir / "formality_butterfly.png")

    # AGRO SECTOR PLOTS
    logger.info("creating agro sector plots")

    # Age distribution for agro
    logger.info("creating age distribution mirror histogram for agro sector")
    df_age_agro = pd.read_csv(processed_dir_agro / "age_distribution.csv", encoding="utf-8-sig")
    age_chart_agro = plot_mirror_histogram_with_excess(df_age_agro)
    age_chart_agro.save(str(output_dir_agro / "age_distribution_mirror_histogram.png"), scale_factor=3.0, ppi=400)
    logger.info(
        "saved agro age distribution mirror histogram to %s", output_dir_agro / "age_distribution_mirror_histogram.png"
    )

    # Employment dumbbell plot for agro
    logger.info("creating employment dumbbell plot for agro sector")
    df_employment_agro = pd.read_csv(processed_dir_agro / "employment.csv", encoding="utf-8-sig")
    employment_dumbbell_agro = plot_employment_dumbbell_by_category(df_employment_agro)
    employment_dumbbell_agro.save(str(output_dir_agro / "employment_dumbbell.png"), scale_factor=3.0, ppi=400)
    logger.info("saved agro employment dumbbell plot to %s", output_dir_agro / "employment_dumbbell.png")

    # Reasons butterfly plot for agro
    logger.info("creating reasons butterfly plot for agro sector")
    df_reasons_agro = pd.read_csv(processed_dir_agro / "reasons.csv", encoding="utf-8-sig")
    reasons_chart_agro = plot_reasons_butterfly(df_reasons_agro)
    reasons_chart_agro.save(str(output_dir_agro / "reasons_butterfly.png"), scale_factor=3.0, ppi=400)
    logger.info("saved agro reasons butterfly plot to %s", output_dir_agro / "reasons_butterfly.png")

    # Formality butterfly plot for agro
    logger.info("creating formality butterfly plot for agro sector")
    df_formality_agro = pd.read_csv(processed_dir_agro / "formality.csv", encoding="utf-8-sig")
    formality_butterfly_agro = plot_formality_by_indicator(df_formality_agro)
    formality_butterfly_agro.save(str(output_dir_agro / "formality_butterfly.png"), scale_factor=3.0, ppi=400)
    logger.info("saved agro formality butterfly plot to %s", output_dir_agro / "formality_butterfly.png")

    # COMBINED PLOTS (Manufacturing + Agro side by side)
    logger.info("creating combined plots for manufacturing and agro sectors")

    # Combined reasons butterfly plot
    logger.info("creating combined reasons butterfly plot")
    df_reasons_manu = pd.read_csv(processed_dir / "reasons.csv", encoding="utf-8-sig")
    df_reasons_agro = pd.read_csv(processed_dir_agro / "reasons.csv", encoding="utf-8-sig")
    reasons_combined = plot_reasons_butterfly_combined(df_reasons_manu, df_reasons_agro)
    reasons_combined.save(str(output_dir_all / "reasons_butterfly_combined.png"), scale_factor=3.0, ppi=400)
    logger.info("saved combined reasons butterfly plot to %s", output_dir_all / "reasons_butterfly_combined.png")

    # Combined formality butterfly plot
    logger.info("creating combined formality butterfly plot")
    df_formality_manu = pd.read_csv(processed_dir / "formality.csv", encoding="utf-8-sig")
    df_formality_agro = pd.read_csv(processed_dir_agro / "formality.csv", encoding="utf-8-sig")
    formality_combined = plot_formality_by_indicator_combined(df_formality_manu, df_formality_agro)
    formality_combined.save(str(output_dir_all / "formality_butterfly_combined.png"), scale_factor=3.0, ppi=400)
    logger.info("saved combined formality butterfly plot to %s", output_dir_all / "formality_butterfly_combined.png")

    # Combined employment dumbbell plot
    logger.info("creating combined employment dumbbell plot")
    df_employment_manu = pd.read_csv(processed_dir / "employment.csv", encoding="utf-8-sig")
    df_employment_agro = pd.read_csv(processed_dir_agro / "employment.csv", encoding="utf-8-sig")
    employment_combined = plot_employment_dumbbell_by_category_combined(df_employment_manu, df_employment_agro)
    employment_combined.save(str(output_dir_all / "employment_dumbbell_combined.png"), scale_factor=3.0, ppi=400)
    logger.info("saved combined employment dumbbell plot to %s", output_dir_all / "employment_dumbbell_combined.png")

    # Formality indicators: all sectors combined
    logger.info("creating formality indicators plot for all sectors combined")
    df_formality_all = pd.read_csv(processed_dir_all / "formality.csv", encoding="utf-8-sig")
    formality_all = plot_formality_by_indicator(df_formality_all)
    formality_all.save(str(output_dir_all / "formality_butterfly_all.png"), scale_factor=3.0, ppi=400)
    logger.info("saved formality butterfly plot for all sectors to %s", output_dir_all / "formality_butterfly_all.png")
