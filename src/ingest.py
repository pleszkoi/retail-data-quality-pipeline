# A type hint-eket “késleltetve” kezeli, vagyis nem azonnal próbálja kiértékelni őket futáskor.
from __future__ import annotations

# Betölti a Python beépített logging modulját.
import logging

# Betölti a Path osztályt, amivel fájl- és mappaútvonalakat kulturáltan lehet kezelni.
from pathlib import Path
# A type hinthez használjuk.
from typing import Dict

# Betölti a pandas könyvtárat pd rövidítéssel.
# A CSV-k beolvasásához és DataFrame-ek kezeléséhez.
import pandas as pd

# Létrehoz egy logger objektumot az aktuális modulhoz.
# __name__: Ez egy speciális Python változó.
# Ebben a fájlban, ha importáljuk, akkor src.ingest lesz.
# ha közvetlenül futtatjuk a fájlt, akkor __main__
# A logban látni fogjuk, honnan jött az üzenet
LOGGER = logging.getLogger(__name__)

# Ez egy dictionary, ami megmondja, hogy datasetenként milyen oszlopokat várunk.
# A pipeline csak akkor tud megbízhatóan működni, ha az input fájlok legalább a várt oszlopokat tartalmazzák.
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

# Megállapítja a projekt gyökérmappáját.
def get_project_root() -> Path:
    """
    Returns the project root directory based on the location of this file.
    """
    # __file__: Ez egy speciális Python változó, ami az aktuális fájl elérési útját tartalmazza.
    # Path(__file__): Stringből Path objektumot csinál
    # .resolve(): Abszolút, feloldott útvonalat készít
    # .parent.parent: Két szinttel feljebb megy, vagyis a projekt gyökeréhez
    return Path(__file__).resolve().parent.parent

# Visszaadja a raw adatokat tartalmazó mappa útvonalát.
def get_raw_data_dir() -> Path:
    """
    Returns the path to the raw data directory.
    """
    return get_project_root() / "data" / "raw"

# Egy Path objektumot kap bemenetként, és egy pandas DataFrame-et ad vissza.
def read_csv_file(file_path: Path) -> pd.DataFrame:
    """
    Reads a CSV file into a pandas DataFrame.

    Raises:
        FileNotFoundError: if the file does not exist
        pd.errors.EmptyDataError: if the file is empty
        pd.errors.ParserError: if the CSV is malformed
    """
    # file_path.exists(): A Path objektum metódusa, ami True-t ad vissza, ha az útvonal létezik.
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    LOGGER.info("Reading file: %s", file_path)

    # Beolvassa a CSV fájlt DataFrame-be.
    # Az eredmény egy pandas DataFrame, ahol:
    #  - oszlopok = CSV fejléc
    #  - sorok = CSV rekordok
    dataframe = pd.read_csv(file_path)

    LOGGER.info("Loaded %s rows from %s", len(dataframe), file_path.name)
    return dataframe

# Egy DataFrame-et kap és a dataset nevét, pl. "customers"
# Semmit (None) nem ad vissza, csak ellenőriz.
# Ha gond van, hibát dob.
def validate_expected_columns(
    dataframe: pd.DataFrame,
    dataset_name: str,
) -> None:
    """
    Validates that the DataFrame contains all expected columns.
    """
    # Kiveszi a EXPECTED_SCHEMAS dictionary-ből az adott datasethez tartozó elvárt oszlopokat.
    expected_columns = EXPECTED_SCHEMAS[dataset_name]
    
    # Lekéri a DataFrame tényleges oszlopait, és listává alakítja.
    actual_columns = dataframe.columns.tolist()

    # list comprehension
    missing_columns = [column for column in expected_columns if column not in actual_columns]

    if missing_columns:
        raise ValueError(
            f"Dataset '{dataset_name}' is missing required columns: {missing_columns}. "
            f"Actual columns: {actual_columns}"
        )

    # Ha nem volt hiányzó oszlop, akkor logolja, hogy a sémaellenőrzés rendben lefutott.
    LOGGER.info("Schema validation passed for dataset: %s", dataset_name)

# Beolvas egy darab datasetet, például a customers-t.
def load_dataset(dataset_name: str) -> pd.DataFrame:
    """
    Loads one dataset from the raw data directory and validates its columns.
    """
    raw_data_dir = get_raw_data_dir()
    file_path = raw_data_dir / f"{dataset_name}.csv"

    # Beolvassa a CSV-t DataFrame-be.
    dataframe = read_csv_file(file_path)
    validate_expected_columns(dataframe, dataset_name)

    return dataframe

# Beolvassa az összes datasetet, és dictionary-ben adja vissza.
def load_all_datasets() -> Dict[str, pd.DataFrame]:
    """
    Loads all raw datasets and returns them in a dictionary.
    """
    # Létrehoz egy üres dictionary-t.
    # Azért van típushint rajta, hogy olvashatóbbá tegye, hogy milyen szerkezetet várunk.
    datasets: Dict[str, pd.DataFrame] = {}

    # Végigiterál a EXPECTED_SCHEMAS kulcsain.
    for dataset_name in EXPECTED_SCHEMAS:
        LOGGER.info("Loading dataset: %s", dataset_name)
        datasets[dataset_name] = load_dataset(dataset_name)

    LOGGER.info("All datasets loaded successfully.")
    return datasets

# Ez azt jelenti:
#  - ha ezt a fájlt közvetlenül futtatjuk, akkor az alatta lévő kód lefut
#  - ha importáljuk máshonnan, akkor nem
if __name__ == "__main__":
    # Beállítja a logging alapkonfigurációját.
    logging.basicConfig(
        # Az INFO és annál súlyosabb logok jelennek meg.
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        loaded_datasets = load_all_datasets()

        # Végigmegy a dictionary elemein.
        # name = kulcs, pl. "customers"
        # dataframe = az adott DataFrame
        for name, dataframe in loaded_datasets.items():
            print(f"\nDataset: {name}")
            print(dataframe.head())
    except Exception as error:
        # Ez speciális logging metódus, ami a traceback-et is logolja.
        LOGGER.exception("Ingestion failed: %s", error)
        raise
