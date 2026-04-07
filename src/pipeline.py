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

# Biztosítja, hogy a szükséges output mappák létezzenek.
def ensure_output_directories() -> None:
    """
    Create output directories if they do not exist.
    """
    get_processed_dir().mkdir(parents=True, exist_ok=True)
    get_rejected_dir().mkdir(parents=True, exist_ok=True)
    get_logs_dir().mkdir(parents=True, exist_ok=True)

# Elmenti a validáció eredményeit CSV fájlokba.
def save_validation_reports(validation_results: dict[str, pd.DataFrame]) -> None:
    """
    Save validation result dataframes to CSV files.
    """
    logs_dir = get_logs_dir()
    
    # Végigmegy a dictionary összes elemén:
    #  - result_name → pl. "customers"
    #  - dataframe → a hozzá tartozó issue DataFrame
    for result_name, dataframe in validation_results.items():
        # Összerakja a kimeneti fájl nevét.
        output_file = logs_dir / f"{result_name}_validation_report.csv"
        # Kiírja a DataFrame-et CSV fájlba.
        # index=False
        # Mert a DataFrame pandas indexét nem akarjuk külön oszlopként elmenteni.
        dataframe.to_csv(output_file, index=False)
        LOGGER.info("Saved validation report: %s", output_file)

# Datasetenként kettéválasztja az adatot:
#  - clean
#  - rejected

# datasets: Ez a transzformált DataFrame-ek dictionary-je.
# validation_results: Ez a validációk eredménye datasetenként.
def split_clean_and_rejected_data(
    datasets: dict[str, pd.DataFrame],
    validation_results: dict[str, pd.DataFrame],
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    Split each dataset into clean and rejected records.

    A record is rejected if it appears in any validation issue for that dataset.
    """
    # Létrehoz egy üres dictionary-t, amibe majd eltárolja a datasetenkénti clean/rejected eredményt.
    split_results: dict[str, dict[str, pd.DataFrame]] = {}

    # Végigmegy az összes dataseten: customers, products, orders
    for dataset_name, dataframe in datasets.items():
        # Lekéri az adott datasethez tartozó issue DataFrame-et.
        # Például ha dataset_name == "customers", akkor a customers issue listát veszi ki.
        
        dataset_issues = validation_results[dataset_name]

        # Ha nincs hiba
        # Ha az adott datasethez nincs egyetlen issue sem:
        #  - az összes rekord clean
        #  - a rejected legyen üres DataFrame
        
        # columns=dataframe.columns
        # Hogy az üres rejected DataFrame-nek is ugyanazok az oszlopai legyenek, mint az eredetinek.
        if dataset_issues.empty:
            clean_df = dataframe.copy()
            rejected_df = pd.DataFrame(columns=dataframe.columns)
        
        # dataset_issues.index.unique()
        # Lekéri azoknak a rekordoknak az eredeti indexét, amelyek valamilyen issue-ban szerepelnek.
        
        # .unique(), mert ugyanaz a rekord több hibát is kaphat.

        # dataframe.loc[rejected_indices]: Kiválasztja azokat a rekordokat, amelyek hibásak.

        # dataframe.drop(index=rejected_indices): Az összes többiből lesz a clean dataset.
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

# A clean és rejected DataFrame-ek mentése fájlokba.
def save_split_datasets(split_results: dict[str, dict[str, pd.DataFrame]]) -> None:
    """
    Save clean and rejected datasets to CSV files.
    """
    # Lekéri a két célmappát.
    processed_dir = get_processed_dir()
    rejected_dir = get_rejected_dir()
    
    # Végigmegy az összes dataset split eredményén.
    for dataset_name, result in split_results.items():
        # Összerakja a fájlneveket.
        clean_output_file = processed_dir / f"clean_{dataset_name}.csv"
        rejected_output_file = rejected_dir / f"rejected_{dataset_name}.csv"

        # Elmenti a két DataFrame-et.
        result["clean"].to_csv(clean_output_file, index=False)
        result["rejected"].to_csv(rejected_output_file, index=False)

        LOGGER.info("Saved clean dataset: %s", clean_output_file)
        LOGGER.info("Saved rejected dataset: %s", rejected_output_file)

# Készít egy összegző táblát datasetenként.
def build_quality_summary(validation_results: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a simple quality summary table from validation results.
    """
    # Ide gyűjtjük majd a sorokat dict formában.
    summary_rows = []

    # Datasetenként végigmegyünk a fő három táblán.
    for dataset_name in ["customers", "products", "orders"]:
        issues_df = validation_results[dataset_name]

        # Ha nincs issue, akkor 0-ás összesítőt teszünk bele.
        # A continue azt jelenti: ugorj a következő iterációra.
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
        
        # Azt nézi, mely sorok severity-je "ERROR".
        # Ez egy boolean sorozat lesz, például: [True, False, True, True]
        # A .sum() pandasban True=1, False=0 alapon összeszámolja őket.
        error_count = (issues_df["severity"] == "ERROR").sum()
        
        # Ugyanez warningokra.
        warning_count = (issues_df["severity"] == "WARNING").sum()

        # Ez hozzáad egy dictionary-t az összegző listához.
        summary_rows.append(
            {
                "dataset_name": dataset_name,
                "issue_count": len(issues_df),
                "error_count": int(error_count),
                "warning_count": int(warning_count),
            }
        )
    # Visszaalakítás DataFrame-be
    # A listányi dictionary-ből DataFrame-et csinál.
    # Ez lesz a quality_summary.csv alapja.
    return pd.DataFrame(summary_rows)

# A quality summary DataFrame mentése.
def save_quality_summary(summary_df: pd.DataFrame) -> None:
    """
    Save the quality summary to CSV.
    """
    output_file = get_logs_dir() / "quality_summary.csv"
    summary_df.to_csv(output_file, index=False)
    LOGGER.info("Saved quality summary: %s", output_file)

# Ez a pipeline fő belépési pontja.
# A cél a teljes data pipeline folyamat futtatása a helyes sorrendben.
def run_pipeline() -> None:
    """
    Run the full ingestion + validation + split pipeline.
    """
    LOGGER.info("Pipeline started.")

    # Biztosítja, hogy a célmappák léteznek.
    ensure_output_directories()

    # Ez az ingest lépés.
    # Beolvassa a raw adatokat.
    datasets = load_all_datasets()

    # Ez a transform lépés.
    # A raw adatokat standardizálja, megtisztítja, típusosítja.
    transformed_datasets = transform_datasets(datasets)

    # Ez a validate lépés.
    # A transzformált adatokon lefuttatja a quality szabályokat.
    validation_results = run_validations(transformed_datasets)

    # Elmenti a részletes validációs riportokat.
    save_validation_reports(validation_results)

    # A validáció alapján clean és rejected részre bontja az adatot.
    split_results = split_clean_and_rejected_data(transformed_datasets, validation_results)
    # Elmenti a clean és rejected outputokat CSV-be.
    save_split_datasets(split_results)

    # Létrehozza és elmenti az összegző quality summary-t.
    quality_summary_df = build_quality_summary(validation_results)
    save_quality_summary(quality_summary_df)

    LOGGER.info("Pipeline finished successfully.")


if __name__ == "__main__":
    # Beállítja a log formátumot és szintet.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    # Megpróbálja lefuttatni a pipeline-t.
    # Ha hiba történik:
    #  - logolja a hibát tracebackkel együtt
    #  - újradobja a kivételt
    try:
        run_pipeline()
    except Exception as error:
        LOGGER.exception("Pipeline execution failed: %s", error)
        raise
