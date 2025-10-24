"""Analyze employment data across ZASCA, EMICRON, and RUES sources."""

import logging

import numpy as np
import pandas as pd

from data_processing.utils import apply_sector_filter

logger = logging.getLogger("innpulsa.scripts.descriptive.data_processing.employment")


@apply_sector_filter
def employment(
    df_zasca: pd.DataFrame,
    df_personal_ocupado: pd.DataFrame,
    df_rues: pd.DataFrame,
    filtro_por_sector: str = "manufactura",
) -> pd.DataFrame:
    """Analyze employment data across ZASCA, EMICRON, and RUES sources.

    Args:
        df_zasca: ZASCA dataframe
        df_personal_ocupado: EMICRON personal ocupado dataframe
        df_rues: RUES dataframe
        filtro_por_sector: sector filter to apply

    Returns:
        pd.DataFrame: processed employment analysis data

    """
    logger.info("processing employment data for %s sector", filtro_por_sector)

    rng = np.random.default_rng(42)

    # process ZASCA employment data
    zasca_employment = _process_zasca_employment(df_zasca)

    # process EMICRON employment data with weighted sampling
    emicron_employment = _process_emicron_employment(df_personal_ocupado, rng)

    # process RUES employment data
    rues_employment = _process_rues_employment(df_rues, rng)

    # combine all sources
    employment_data = pd.concat(
        [zasca_employment, emicron_employment, rues_employment],
        ignore_index=True,
    )

    logger.info("processed employment data: %d observations", len(employment_data))

    return employment_data


def _process_zasca_employment(df_zasca: pd.DataFrame) -> pd.DataFrame:
    """Process ZASCA employment data.

    Args:
        df_zasca: ZASCA dataframe

    Returns:
        pd.DataFrame: processed ZASCA employment data

    """
    # calculate proportions per business, then average
    df_zasca = df_zasca.copy()

    # filter out businesses with zero employees
    df_zasca = df_zasca.loc[df_zasca["emp_total"] > 0].copy()

    # calculate proportions for each business
    if "employees_w" in df_zasca.columns:
        df_zasca["prop_female"] = df_zasca["employees_w"] / df_zasca["emp_total"]
        df_zasca["prop_male"] = (df_zasca["emp_total"] - df_zasca["employees_w"]) / df_zasca["emp_total"]
    else:
        # Skip gender proportions if employees_w not available
        df_zasca["prop_female"] = np.nan
        df_zasca["prop_male"] = np.nan

    # Handle missing emp_htc column (not available for agro)
    if "emp_ht" in df_zasca.columns:
        df_zasca["prop_temporary"] = df_zasca["emp_ht"] / df_zasca["emp_total"]
        df_zasca["prop_indefinite"] = (df_zasca["emp_total"] - df_zasca["emp_ht"]) / df_zasca["emp_total"]
    else:
        df_zasca["prop_temporary"] = np.nan
        df_zasca["prop_indefinite"] = np.nan

    # Handle missing emp_volc column (not available for agro)
    if "emp_vol" in df_zasca.columns:
        df_zasca["prop_family_unpaid"] = df_zasca["emp_vol"] / df_zasca["emp_total"]
        df_zasca["prop_assalaried"] = (df_zasca["emp_total"] - df_zasca["emp_vol"]) / df_zasca["emp_total"]
    else:
        df_zasca["prop_family_unpaid"] = np.nan
        df_zasca["prop_assalaried"] = np.nan

    # average proportions across all businesses
    avg_prop_female = df_zasca["prop_female"].mean()
    avg_prop_male = df_zasca["prop_male"].mean()
    avg_prop_temporary = df_zasca["prop_temporary"].mean()
    avg_prop_family_unpaid = df_zasca["prop_family_unpaid"].mean()
    avg_prop_indefinite = df_zasca["prop_indefinite"].mean()
    avg_prop_assalaried = df_zasca["prop_assalaried"].mean()

    # create employment data - only include categories with valid data
    employment_data = []

    # Add gender data if available
    if not np.isnan(avg_prop_female) and not np.isnan(avg_prop_male):
        employment_data.extend([
            {
                "source": "ZASCA",
                "category": "Género",
                "subcategory": "Mujer",
                "proportion": avg_prop_female,
            },
            {
                "source": "ZASCA",
                "category": "Género",
                "subcategory": "Hombre",
                "proportion": avg_prop_male,
            },
        ])

    # Add contract duration data if available
    if not np.isnan(avg_prop_temporary) and not np.isnan(avg_prop_indefinite):
        employment_data.extend([
            {
                "source": "ZASCA",
                "category": "Duración del Contrato",
                "subcategory": "Temporal",
                "proportion": avg_prop_temporary,
            },
            {
                "source": "ZASCA",
                "category": "Duración del Contrato",
                "subcategory": "Indefinido",
                "proportion": avg_prop_indefinite,
            },
        ])

    # Add employment type data if available
    if not np.isnan(avg_prop_family_unpaid) and not np.isnan(avg_prop_assalaried):
        employment_data.extend([
            {
                "source": "ZASCA",
                "category": "Tipo de Empleo",
                "subcategory": "Asalariado",
                "proportion": avg_prop_assalaried,
            },
            {
                "source": "ZASCA",
                "category": "Tipo de Empleo",
                "subcategory": "Familiar/Sin Pago",
                "proportion": avg_prop_family_unpaid,
            },
        ])

    return pd.DataFrame(employment_data)


def _process_emicron_employment(df_personal_ocupado: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Process EMICRON employment data using expansion factors.

    Args:
        df_personal_ocupado: EMICRON personal ocupado dataframe
        rng: random number generator

    Returns:
        pd.DataFrame: processed EMICRON employment data

    """
    # weighted sampling for EMICRON using expansion factors
    weights = df_personal_ocupado["FEX_C"] / df_personal_ocupado["FEX_C"].sum()
    sample_size = min(5000, len(df_personal_ocupado))
    sample_indices = rng.choice(df_personal_ocupado.index, size=sample_size, replace=True, p=weights)
    df_sample = df_personal_ocupado.loc[sample_indices].copy()

    # calculate weighted totals
    total_emp = df_sample["FEX_C"].sum()

    # gender proportions (P3078: 1=male, 2=female)
    women_emp = df_sample.loc[df_sample["P3078"] == 2, "FEX_C"].sum()  # noqa: PLR2004
    men_emp = df_sample.loc[df_sample["P3078"] == 1, "FEX_C"].sum()

    # contract duration proportions (P3077: 1=indefinite, 2=temporary)
    df_contract = df_sample.dropna(subset=["P3077"])
    indefinite_emp = df_contract.loc[df_contract["P3077"] == 1, "FEX_C"].sum()
    temporary_emp = df_contract.loc[df_contract["P3077"] == 2, "FEX_C"].sum()  # noqa: PLR2004
    contract_total = indefinite_emp + temporary_emp

    # employment type proportions (TIPO: 1=assalaried, 2=partners, 3=workers/family without pay)
    assalaried_emp = df_sample.loc[df_sample["TIPO"] == 1, "FEX_C"].sum()
    family_unpaid_emp = df_sample.loc[df_sample["TIPO"] == 3, "FEX_C"].sum()  # noqa: PLR2004
    employed_without_partners = assalaried_emp + family_unpaid_emp

    # create employment data
    employment_data = [
        {
            "source": "EMICRON",
            "category": "Género",
            "subcategory": "Mujer",
            "proportion": women_emp / total_emp if total_emp > 0 else 0,
        },
        {
            "source": "EMICRON",
            "category": "Género",
            "subcategory": "Hombre",
            "proportion": men_emp / total_emp if total_emp > 0 else 0,
        },
        {
            "source": "EMICRON",
            "category": "Duración del Contrato",
            "subcategory": "Indefinido",
            "proportion": indefinite_emp / contract_total if contract_total > 0 else 0,
        },
        {
            "source": "EMICRON",
            "category": "Duración del Contrato",
            "subcategory": "Temporal",
            "proportion": temporary_emp / contract_total if contract_total > 0 else 0,
        },
        {
            "source": "EMICRON",
            "category": "Tipo de Empleo",
            "subcategory": "Asalariado",
            "proportion": (assalaried_emp / employed_without_partners) if employed_without_partners > 0 else 0,
        },
        {
            "source": "EMICRON",
            "category": "Tipo de Empleo",
            "subcategory": "Familiar/Sin Pago",
            "proportion": (family_unpaid_emp / employed_without_partners) if employed_without_partners > 0 else 0,
        },
    ]

    return pd.DataFrame(employment_data)


def _process_rues_employment(df_rues: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Process RUES employment data.

    Args:
        df_rues: RUES dataframe
        rng: random number generator

    Returns:
        pd.DataFrame: processed RUES employment data

    """
    # create representative sample for RUES
    sample_size = min(5000, len(df_rues))
    sample_indices = rng.choice(df_rues.index, size=sample_size, replace=False)
    df_sample = df_rues.loc[sample_indices].copy()

    # calculate proportions per firm, then average
    df_sample = df_sample.copy()

    # filter out firms with zero employees
    df_sample = df_sample[df_sample["empleados"] > 0].copy()

    # calculate proportions for each firm
    df_sample["prop_female"] = df_sample["cantidad_mujeres_empleadas"] / df_sample["empleados"]
    df_sample["prop_male"] = (df_sample["empleados"] - df_sample["cantidad_mujeres_empleadas"]) / df_sample["empleados"]

    # average proportions across all firms
    avg_prop_female = df_sample["prop_female"].mean()
    avg_prop_male = df_sample["prop_male"].mean()

    # create employment data
    employment_data = [
        {
            "source": "RUES",
            "category": "Género",
            "subcategory": "Mujer",
            "proportion": avg_prop_female,
        },
        {
            "source": "RUES",
            "category": "Género",
            "subcategory": "Hombre",
            "proportion": avg_prop_male,
        },
    ]

    return pd.DataFrame(employment_data)
