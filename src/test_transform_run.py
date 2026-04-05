import logging

from src.ingest import load_all_datasets
from src.transform import transform_datasets


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

datasets = load_all_datasets()
transformed = transform_datasets(datasets)

for dataset_name, dataframe in transformed.items():
    print(f"\n=== {dataset_name} ===")
    print(dataframe.dtypes)
    print(dataframe.head())