import logging

from src.ingest import load_all_datasets
from src.validate import run_validations


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

datasets = load_all_datasets()
validation_results = run_validations(datasets)

for result_name, result_df in validation_results.items():
    print(f"\n=== {result_name} ===")
    if result_df.empty:
        print("No issues found.")
    else:
        print(result_df.head(20))
        print(f"Total issues: {len(result_df)}")