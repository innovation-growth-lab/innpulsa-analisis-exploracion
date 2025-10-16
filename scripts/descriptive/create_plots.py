"""Main script for creating plots."""

import logging
from pathlib import Path

import pandas as pd

from innpulsa.settings import DATA_DIR
from plots.mirror_histogram import plot_mirror_histogram_with_excess
from plots.marimekko_chart import plot_marimekko_gender_comparison

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
