from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import pandas as pd


LOGGER = logging.getLogger(__name__)


EXPECTED_SCHEMAS = {
    "customers": [
        "customer_id",
        "full_name",
        "email",
        "country",
        "registration_date",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "price",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "total_amount",
        "currency",
    ],
}


def get_project_root() -> Path:
    """
    Returns the project root directory based on the location of this file.
    """
    return Path(__file__).resolve().parent.parent


def get_raw_data_dir() -> Path:
    """
    Returns the path to the raw data directory.
    """
    return get_project_root() / "data" / "raw"


def read_csv_file(file_path: Path) -> pd.DataFrame:
    """
    Reads a CSV file into a pandas DataFrame.

    Raises:
        FileNotFoundError: if the file does not exist
        pd.errors.EmptyDataError: if the file is empty
        pd.errors.ParserError: if the CSV is malformed
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    LOGGER.info("Reading file: %s", file_path)
    dataframe = pd.read_csv(file_path)
    LOGGER.info("Loaded %s rows from %s", len(dataframe), file_path.name)
    return dataframe


def validate_expected_columns(
    dataframe: pd.DataFrame,
    dataset_name: str,
) -> None:
    """
    Validates that the DataFrame contains all expected columns.
    """
    expected_columns = EXPECTED_SCHEMAS[dataset_name]
    actual_columns = dataframe.columns.tolist()

    missing_columns = [column for column in expected_columns if column not in actual_columns]

    if missing_columns:
        raise ValueError(
            f"Dataset '{dataset_name}' is missing required columns: {missing_columns}. "
            f"Actual columns: {actual_columns}"
        )

    LOGGER.info("Schema validation passed for dataset: %s", dataset_name)


def load_dataset(dataset_name: str) -> pd.DataFrame:
    """
    Loads one dataset from the raw data directory and validates its columns.
    """
    raw_data_dir = get_raw_data_dir()
    file_path = raw_data_dir / f"{dataset_name}.csv"

    dataframe = read_csv_file(file_path)
    validate_expected_columns(dataframe, dataset_name)

    return dataframe


def load_all_datasets() -> Dict[str, pd.DataFrame]:
    """
    Loads all raw datasets and returns them in a dictionary.
    """
    datasets: Dict[str, pd.DataFrame] = {}

    for dataset_name in EXPECTED_SCHEMAS:
        LOGGER.info("Loading dataset: %s", dataset_name)
        datasets[dataset_name] = load_dataset(dataset_name)

    LOGGER.info("All datasets loaded successfully.")
    return datasets


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        loaded_datasets = load_all_datasets()

        for name, dataframe in loaded_datasets.items():
            print(f"\nDataset: {name}")
            print(dataframe.head())
    except Exception as error:
        LOGGER.exception("Ingestion failed: %s", error)
        raise
