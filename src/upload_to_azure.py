from __future__ import annotations

import logging
import os
from pathlib import Path
# Ez az Azure Blob Storage kliens fő belépési pontja.
from azure.storage.blob import BlobServiceClient


LOGGER = logging.getLogger(__name__)


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_raw_dir() -> Path:
    return get_project_root() / "data" / "raw"


def get_processed_dir() -> Path:
    return get_project_root() / "data" / "processed"


def get_rejected_dir() -> Path:
    return get_project_root() / "data" / "rejected"


def get_logs_dir() -> Path:
    return get_project_root() / "logs"


def get_blob_service_client() -> BlobServiceClient:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")
    return BlobServiceClient.from_connection_string(connection_string)


def upload_file(
    # Az Azure kliens
    blob_service_client: BlobServiceClient,
    container_name: str,
    local_file_path: Path,
    # Milyen néven kerüljön fel Azure-ba, például: raw/orders.csv
    blob_name: str,
) -> None:
    # Ez létrehozza a konkrét blobhoz tartozó klienst.
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # "rb", mert a fájlt bináris olvasásra nyitod meg.
    # Még ha CSV is, a kliensnek byte streamként adjuk át.
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    LOGGER.info("Uploaded %s -> %s/%s", local_file_path, container_name, blob_name)


def upload_directory_files(
    blob_service_client: BlobServiceClient,
    container_name: str,
    source_dir: Path,
    # Az Azure oldali “mappaszerű” prefix, például:
    #  - raw
    #  - processed
    blob_prefix: str,
    pattern: str = "*",
) -> None:
    for file_path in source_dir.glob(pattern):
        if file_path.is_file():
            blob_name = f"{blob_prefix}/{file_path.name}"
            upload_file(blob_service_client, container_name, file_path, blob_name)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    container_name = "retail-data-quality"
    blob_service_client = get_blob_service_client()

    upload_directory_files(blob_service_client, container_name, get_raw_dir(), "raw", "*.csv")
    upload_directory_files(blob_service_client, container_name, get_processed_dir(), "processed", "*.csv")
    upload_directory_files(blob_service_client, container_name, get_rejected_dir(), "rejected", "*.csv")
    upload_directory_files(blob_service_client, container_name, get_logs_dir(), "reports", "*.csv")

    LOGGER.info("Azure upload completed successfully.")


if __name__ == "__main__":
    main()
