from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.ingest import load_all_datasets

from src.transform import transform_datasets
from src.validate import run_validations


LOGGER = logging.getLogger(__name__)


def get_project_root() -> Path:
    """
    Return the project root directory.
    """
    return Path(__file__).resolve().parent.parent


def get_processed_dir() -> Path:
    """
    Return the processed output directory.
    """
    return get_project_root() / "data" / "processed"


def get_rejected_dir() -> Path:
    """
    Return the rejected output directory.
    """
    return get_project_root() / "data" / "rejected"


def get_logs_dir() -> Path:
    """
    Return the logs directory.
    """
    return get_project_root() / "logs"


def ensure_output_directories() -> None:
    """
    Create output directories if they do not exist.
    """
    get_processed_dir().mkdir(parents=True, exist_ok=True)
    get_rejected_dir().mkdir(parents=True, exist_ok=True)
    get_logs_dir().mkdir(parents=True, exist_ok=True)


def save_validation_reports(validation_results: dict[str, pd.DataFrame]) -> None:
    """
    Save validation result dataframes to CSV files.
    """
    logs_dir = get_logs_dir()

    for result_name, dataframe in validation_results.items():
        output_file = logs_dir / f"{result_name}_validation_report.csv"
        dataframe.to_csv(output_file, index=False)
        LOGGER.info("Saved validation report: %s", output_file)


def split_clean_and_rejected_data(
    datasets: dict[str, pd.DataFrame],
    validation_results: dict[str, pd.DataFrame],
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    Split each dataset into clean and rejected records.

    A record is rejected if it appears in any validation issue for that dataset.
    """
    split_results: dict[str, dict[str, pd.DataFrame]] = {}

    for dataset_name, dataframe in datasets.items():
        dataset_issues = validation_results[dataset_name]

        if dataset_issues.empty:
            clean_df = dataframe.copy()
            rejected_df = pd.DataFrame(columns=dataframe.columns)
        else:
            rejected_indices = dataset_issues.index.unique()
            rejected_df = dataframe.loc[rejected_indices].copy()
            clean_df = dataframe.drop(index=rejected_indices).copy()

        split_results[dataset_name] = {
            "clean": clean_df,
            "rejected": rejected_df,
        }

        LOGGER.info(
            "Dataset '%s' split completed - clean rows: %s, rejected rows: %s",
            dataset_name,
            len(clean_df),
            len(rejected_df),
        )

    return split_results


def save_split_datasets(split_results: dict[str, dict[str, pd.DataFrame]]) -> None:
    """
    Save clean and rejected datasets to CSV files.
    """
    processed_dir = get_processed_dir()
    rejected_dir = get_rejected_dir()

    for dataset_name, result in split_results.items():
        clean_output_file = processed_dir / f"clean_{dataset_name}.csv"
        rejected_output_file = rejected_dir / f"rejected_{dataset_name}.csv"

        result["clean"].to_csv(clean_output_file, index=False)
        result["rejected"].to_csv(rejected_output_file, index=False)

        LOGGER.info("Saved clean dataset: %s", clean_output_file)
        LOGGER.info("Saved rejected dataset: %s", rejected_output_file)


def build_quality_summary(validation_results: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a simple quality summary table from validation results.
    """
    summary_rows = []

    for dataset_name in ["customers", "products", "orders"]:
        issues_df = validation_results[dataset_name]

        if issues_df.empty:
            summary_rows.append(
                {
                    "dataset_name": dataset_name,
                    "issue_count": 0,
                    "error_count": 0,
                    "warning_count": 0,
                }
            )
            continue

        error_count = (issues_df["severity"] == "ERROR").sum()
        warning_count = (issues_df["severity"] == "WARNING").sum()

        summary_rows.append(
            {
                "dataset_name": dataset_name,
                "issue_count": len(issues_df),
                "error_count": int(error_count),
                "warning_count": int(warning_count),
            }
        )

    return pd.DataFrame(summary_rows)


def save_quality_summary(summary_df: pd.DataFrame) -> None:
    """
    Save the quality summary to CSV.
    """
    output_file = get_logs_dir() / "quality_summary.csv"
    summary_df.to_csv(output_file, index=False)
    LOGGER.info("Saved quality summary: %s", output_file)


def run_pipeline() -> None:
    """
    Run the full ingestion + validation + split pipeline.
    """
    LOGGER.info("Pipeline started.")

    ensure_output_directories()

    datasets = load_all_datasets()
    transformed_datasets = transform_datasets(datasets)

    validation_results = run_validations(transformed_datasets)

    save_validation_reports(validation_results)

    split_results = split_clean_and_rejected_data(transformed_datasets, validation_results)
    save_split_datasets(split_results)

    quality_summary_df = build_quality_summary(validation_results)
    save_quality_summary(quality_summary_df)

    LOGGER.info("Pipeline finished successfully.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        run_pipeline()
    except Exception as error:
        LOGGER.exception("Pipeline execution failed: %s", error)
        raise
