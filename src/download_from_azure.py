from __future__ import annotations

import logging
# A környezeti változók eléréséhez kell.
import os
from pathlib import Path
# Ez az Azure Blob Storage Python SDK egyik központi osztálya.
# Ez a belépési pont az Azure storage accounthoz.
# Ezzel lehet például:
#  - container clientet kérni
#  - blob clientet kérni
#  - blobokat listázni
#  - blobokat letölteni
from azure.storage.blob import BlobServiceClient


LOGGER = logging.getLogger(__name__)


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_download_root_dir() -> Path:
    return get_project_root() / "data" / "azure_downloads"

# Ez a függvény hozza létre a kapcsolatot az Azure storage accounttal.
def get_blob_service_client() -> BlobServiceClient:
    # os.getenv(...)
    # Ez kiolvassa a shellben beállított környezeti változót.
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")
    # Ez létrehozza a BlobServiceClient objektumot a connection string alapján.
    return BlobServiceClient.from_connection_string(connection_string)

# Ez biztosítja, hogy a célmappa létezzen.
def ensure_local_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

# Egyetlen blob letöltése
def download_blob_to_file(
    # Az Azure kapcsolat.
    blob_service_client: BlobServiceClient,
    # Melyik containerből dolgozunk
    container_name: str,
    # A blob teljes neve a containeren belül
    blob_name: str,
    # Az a helyi útvonal, ahová menteni fog
    local_file_path: Path,
) -> None:
    # Ez létrehoz egy blob-szintű klienst.
    # A BlobServiceClient az egész storage account szintjén dolgozik, de a konkrét letöltéshez kell egy konkrét blob client.
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name,
    )

    ensure_local_directory(local_file_path.parent)
    # Fájl megnyitása bináris írásra
    # "wb", mert blobot tölt le, tehát bináris adatfolyamként kell kezelni.
    # Még ha CSV is, a letöltés alacsonyabb szinten bytes formában jön.
    with open(local_file_path, "wb") as file:
        # download_blob(): Elindítja a letöltést Azure-ból.
        # readall(): Beolvassa a teljes blob tartalmát memóriába.
        # file.write(...): Kiírja helyi fájlba. Tehát a teljes blob tartalma bekerül a helyi fájlba.
        download_stream = blob_client.download_blob()
        file.write(download_stream.readall())

    LOGGER.info("Downloaded %s -> %s", blob_name, local_file_path)

# Ez a függvény egy adott Azure prefix alatti CSV-ket tölti le.
def download_csv_files_from_prefix(
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_prefix: str,
    local_target_dir: Path,
) -> None:
    # Ez a container szintű kliens. Ezzel tudjuk listázni a container tartalmát.
    container_client = blob_service_client.get_container_client(container_name)

    # Ez csak azokat a blobokat listázza, amelyek neve a megadott prefixszel kezdődik.
    blob_list = container_client.list_blobs(name_starts_with=blob_prefix)

    downloaded_file_count = 0

    # A blob itt egy Azure blob metadata objektum, amiből a .name a teljes blobnév.
    for blob in blob_list:
        blob_name = blob.name
        # Csak .csv fájlok engedése
        if not blob_name.endswith(".csv"):
            continue
        # Kiszedi csak a fájlnevet a blob útvonalból.
        file_name = Path(blob_name).name
        # Összerakja a helyi célfájlt.
        local_file_path = local_target_dir / file_name

        download_blob_to_file(
            blob_service_client=blob_service_client,
            container_name=container_name,
            blob_name=blob_name,
            local_file_path=local_file_path,
        )

        downloaded_file_count += 1

    LOGGER.info(
        "Downloaded %s CSV files from prefix '%s' into %s",
        downloaded_file_count,
        blob_prefix,
        local_target_dir,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    container_name = "retail-data-quality"
    download_root_dir = get_download_root_dir()
    blob_service_client = get_blob_service_client()

    download_csv_files_from_prefix(
        blob_service_client=blob_service_client,
        container_name=container_name,
        blob_prefix="raw/",
        local_target_dir=download_root_dir / "raw",
    )

    download_csv_files_from_prefix(
        blob_service_client=blob_service_client,
        container_name=container_name,
        blob_prefix="processed/",
        local_target_dir=download_root_dir / "processed",
    )

    download_csv_files_from_prefix(
        blob_service_client=blob_service_client,
        container_name=container_name,
        blob_prefix="rejected/",
        local_target_dir=download_root_dir / "rejected",
    )

    download_csv_files_from_prefix(
        blob_service_client=blob_service_client,
        container_name=container_name,
        blob_prefix="reports/",
        local_target_dir=download_root_dir / "reports",
    )

    LOGGER.info("Azure download completed successfully.")


if __name__ == "__main__":
    main()
