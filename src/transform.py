from __future__ import annotations

import logging
from typing import Dict

import pandas as pd


LOGGER = logging.getLogger(__name__)

# Whitespace tisztítás szöveges mezőkön.
# pd.Series
# A pandas Series egy oszlopnyi adat. Tehát ez a függvény egy teljes oszlopot kap bemenetként.
def _clean_string_series(series: pd.Series) -> pd.Series:
    """
    Strip whitespace from string values while keeping missing values untouched.
    """
    # apply(...)
    # Sorban végigmegy az oszlop összes elemén, és minden elemre lefuttat egy függvényt.

    # lambda value: value.strip() if isinstance(value, str) else value
    #  - ha az érték string, akkor vágd le a szóközöket az elejéről és végéről
    #  - különben hagyd változatlanul
    return series.apply(lambda value: value.strip() if isinstance(value, str) else value)

# A customers dataset alap egységesítése.
def transform_customers(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Apply basic transformations to the customers dataset.
    """
    # Azért, hogy ne az eredeti DataFrame-et módosítsuk közvetlenül.
    transformed_df = dataframe.copy()

    # Három oszlopon whitespace tisztítást végez.
    transformed_df["full_name"] = _clean_string_series(transformed_df["full_name"])
    transformed_df["email"] = _clean_string_series(transformed_df["email"])
    transformed_df["country"] = _clean_string_series(transformed_df["country"])

    # Az email címeket kisbetűssé alakítja.
    transformed_df["email"] = transformed_df["email"].apply(
        lambda value: value.lower() if isinstance(value, str) else value
    )

    # A country mezőt nagybetűssé alakítja.
    transformed_df["country"] = transformed_df["country"].apply(
        lambda value: value.upper() if isinstance(value, str) else value
    )

    # A registration_date oszlopot dátum/idő típusra alakítja.
    transformed_df["registration_date"] = pd.to_datetime(
        transformed_df["registration_date"],
        errors="coerce",
    )

    LOGGER.info("Customers dataset transformed. Rows: %s", len(transformed_df))
    return transformed_df

# A products dataset egységesítése.
def transform_products(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Apply basic transformations to the products dataset.
    """
    transformed_df = dataframe.copy()

    transformed_df["product_name"] = _clean_string_series(transformed_df["product_name"])
    transformed_df["category"] = _clean_string_series(transformed_df["category"])

    # A category mezőt “Title Case”-re alakítja.
    transformed_df["category"] = transformed_df["category"].apply(
        lambda value: value.title() if isinstance(value, str) else value
    )

    # A price mezőt számmá alakítja.
    transformed_df["price"] = pd.to_numeric(transformed_df["price"], errors="coerce")

    LOGGER.info("Products dataset transformed. Rows: %s", len(transformed_df))
    return transformed_df

# Az orders dataset egységesítése.
def transform_orders(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Apply basic transformations to the orders dataset.
    """
    transformed_df = dataframe.copy()

    transformed_df["order_date"] = pd.to_datetime(
        transformed_df["order_date"],
        errors="coerce",
    )

    transformed_df["currency"] = _clean_string_series(transformed_df["currency"])
    transformed_df["currency"] = transformed_df["currency"].apply(
        lambda value: value.upper() if isinstance(value, str) else value
    )

    transformed_df["quantity"] = pd.to_numeric(transformed_df["quantity"], errors="coerce")
    transformed_df["total_amount"] = pd.to_numeric(transformed_df["total_amount"], errors="coerce")

    LOGGER.info("Orders dataset transformed. Rows: %s", len(transformed_df))
    return transformed_df


def transform_datasets(datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform all datasets and return them in a dictionary.
    """
    transformed = {
        "customers": transform_customers(datasets["customers"]),
        "products": transform_products(datasets["products"]),
        "orders": transform_orders(datasets["orders"]),
    }

    LOGGER.info("All datasets transformed successfully.")
    return transformed
