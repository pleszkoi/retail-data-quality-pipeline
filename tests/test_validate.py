import pandas as pd

from src.validate import (
    validate_date_column,
    validate_email_column,
    validate_foreign_key,
    validate_numeric_min_value,
    validate_required_column,
    validate_unique_column,
)


def test_validate_required_column_detects_missing_and_empty_values() -> None:
    dataframe = pd.DataFrame(
        {
            "full_name": ["John Doe", None, "", "   ", "Jane Smith"],
        }
    )

    issues = validate_required_column(
        dataframe=dataframe,
        dataset_name="customers",
        column_name="full_name",
        severity="ERROR",
    )

    assert len(issues) == 3
    assert (issues["rule_name"] == "required_column").all()
    assert (issues["severity"] == "ERROR").all()


def test_validate_unique_column_detects_duplicates() -> None:
    dataframe = pd.DataFrame(
        {
            "customer_id": [1, 2, 2, 3],
        }
    )

    issues = validate_unique_column(
        dataframe=dataframe,
        dataset_name="customers",
        column_name="customer_id",
        severity="ERROR",
    )

    assert len(issues) == 2
    assert (issues["column_name"] == "customer_id").all()


def test_validate_date_column_detects_invalid_dates() -> None:
    dataframe = pd.DataFrame(
        {
            "order_date": ["2023-08-01", "invalid_date", None],
        }
    )

    issues = validate_date_column(
        dataframe=dataframe,
        dataset_name="orders",
        column_name="order_date",
        severity="ERROR",
    )

    assert len(issues) == 1
    assert issues.iloc[0]["order_date"] == "invalid_date"


def test_validate_numeric_min_value_detects_invalid_numbers() -> None:
    dataframe = pd.DataFrame(
        {
            "quantity": [1, 2, 0, -1, None],
        }
    )

    issues = validate_numeric_min_value(
        dataframe=dataframe,
        dataset_name="orders",
        column_name="quantity",
        min_value=1,
        severity="ERROR",
    )

    assert len(issues) == 3
    assert (issues["rule_name"] == "numeric_min_value").all()


def test_validate_email_column_detects_invalid_email_format() -> None:
    dataframe = pd.DataFrame(
        {
            "email": ["valid@email.com", "invalid_email", None, "also@valid.com"],
        }
    )

    issues = validate_email_column(
        dataframe=dataframe,
        dataset_name="customers",
        column_name="email",
        severity="WARNING",
    )

    assert len(issues) == 1
    assert issues.iloc[0]["email"] == "invalid_email"
    assert issues.iloc[0]["severity"] == "WARNING"


def test_validate_foreign_key_detects_missing_reference() -> None:
    orders_df = pd.DataFrame(
        {
            "customer_id": [1, 2, 999],
        }
    )

    customers_df = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
        }
    )

    issues = validate_foreign_key(
        dataframe=orders_df,
        reference_dataframe=customers_df,
        dataset_name="orders",
        column_name="customer_id",
        reference_column="customer_id",
        reference_dataset_name="customers",
        severity="ERROR",
    )

    assert len(issues) == 1
    assert issues.iloc[0]["customer_id"] == 999
