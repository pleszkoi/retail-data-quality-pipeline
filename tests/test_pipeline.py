from pathlib import Path

from src.pipeline import run_pipeline


def test_run_pipeline_creates_expected_output_files() -> None:
    run_pipeline()

    project_root = Path(__file__).resolve().parent.parent

    expected_files = [
        project_root / "data" / "processed" / "clean_customers.csv",
        project_root / "data" / "processed" / "clean_products.csv",
        project_root / "data" / "processed" / "clean_orders.csv",
        project_root / "data" / "rejected" / "rejected_customers.csv",
        project_root / "data" / "rejected" / "rejected_products.csv",
        project_root / "data" / "rejected" / "rejected_orders.csv",
        project_root / "logs" / "quality_summary.csv",
        project_root / "logs" / "all_issues_validation_report.csv",
    ]

    for file_path in expected_files:
        assert file_path.exists(), f"Expected output file does not exist: {file_path}"
