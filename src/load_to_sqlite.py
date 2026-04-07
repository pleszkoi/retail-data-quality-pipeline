from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_database_path() -> Path:
    return get_project_root() / "data" / "sqlite" / "retail_pipeline.db"


def get_processed_dir() -> Path:
    return get_project_root() / "data" / "processed"


def get_sql_dir() -> Path:
    return get_project_root() / "sql"


def read_sql_file(file_path: Path) -> str:
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def load_clean_csvs_to_sqlite(connection: sqlite3.Connection) -> None:
    processed_dir = get_processed_dir()

    customers_df = pd.read_csv(processed_dir / "clean_customers.csv")
    products_df = pd.read_csv(processed_dir / "clean_products.csv")
    orders_df = pd.read_csv(processed_dir / "clean_orders.csv")

    customers_df.to_sql("clean_customers", connection, if_exists="replace", index=False)
    products_df.to_sql("clean_products", connection, if_exists="replace", index=False)
    orders_df.to_sql("clean_orders", connection, if_exists="replace", index=False)

    LOGGER.info("Clean CSV files loaded into SQLite successfully.")


def execute_sql_script(connection: sqlite3.Connection, script_path: Path) -> None:
    sql_script = read_sql_file(script_path)
    connection.executescript(sql_script)
    LOGGER.info("Executed SQL script: %s", script_path.name)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    database_path = get_database_path()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(database_path) as connection:
        execute_sql_script(connection, get_sql_dir() / "create_tables.sql")
        load_clean_csvs_to_sqlite(connection)
        execute_sql_script(connection, get_sql_dir() / "kpi_queries.sql")

    LOGGER.info("SQLite data load and KPI creation completed successfully.")


if __name__ == "__main__":
    main()