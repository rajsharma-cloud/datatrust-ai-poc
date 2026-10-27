"""
Azure Data Lake Storage Gen2 helper functions.

Uses the azure-storage-blob SDK, which works with ADLS Gen2 accounts (the blob endpoint
works on accounts with hierarchical namespace) and with the free Azurite emulator.

  1. ADLS_CONNECTION_STRING
  2. ADLS_ACCOUNT_NAME + ADLS_ACCOUNT_KEY
  3. ADLS_ACCOUNT_NAME only -> your Azure AD login (run `az login` once; needs the
     "Storage Blob Data Contributor" role on the account)
(lists the files in the container folder)
"""

from pathlib import Path
from azure.storage.blob import BlobServiceClient, ContainerClient
import config

def get_adls_client() -> BlobServiceClient:
        """Create the connection to the storage account."""
        if config.ADLS_CONNECTION_STRING:
                return BlobServiceClient.from_connection_string(config.ADLS_CONNECTION_STRING)

        if not config.ADLS_ACCOUNT_NAME:
               raise ValueError("Set ADLS_ACCOUNT_NAME (and ADLS_ACCOUNT_KEY) or ADLS_CONNECTION_STRING in .env")

        # ADLS path
        url = f"https://{config.ADLS_ACCOUNT_NAME}.blob.core.windows.net"
        if config.ADLS_ACCOUNT_KEY:
               return BlobServiceClient(account_url=url, credential=config.ADLS_ACCOUNT_KEY)

        from azure.identity import DefaultAzureCredential      # Azure AD login (az login / VS Code Azure sign-in)
        return BlobServiceClient(account_url=url, credential=DefaultAzureCredential())


def get_container(create_if_missing: bool = False) -> ContainerClient:
    """Get the container (file system) that holds the landing files."""
    container = get_adls_client().get_container_client(config.ADLS_CONTAINER)
    if create_if_missing and not container.exists():
        container.create_container()
        print(f"created container {config.ADLS_CONTAINER}")
    return container


def test_connection() -> bool:
    """Return True if the storage account and container can be reached."""
    try:
        exists = get_container().exists()
        print(f"ADLS connection OK. Container '{config.ADLS_CONTAINER}' exists: {exists}")
        return True
    except Exception as e:
        print(f"ADLS connection FAILED: {e}")
        return False

def list_files(folder: str = None) -> list:
    """List file paths under a folder in the container."""
    folder = (folder or config.ADLS_FOLDER).rstrip("/") + "/"
    return [b.name for b in get_container().list_blobs(name_starts_with=folder)]


def upload_file(local_path: Path, adls_path: str) -> None:
    """Upload one local file to ADLS (overwrites if it exists)."""
    with open(local_path, "rb") as f:
        get_container().upload_blob(name=adls_path, data=f, overwrite=True)
    print(f"uploaded  {local_path.name:<22} -> {config.ADLS_CONTAINER}/{adls_path}")


def upload_folder(local_folder: Path, adls_folder: str = None) -> int:
    """Upload every CSV file in a local folder to an ADLS folder."""
    adls_folder = (adls_folder or config.ADLS_FOLDER).rstrip("/")
    get_container(create_if_missing=True)
    files = sorted(Path(local_folder).glob("*.csv"))
    for f in files:
        upload_file(f, f"{adls_folder}/{f.name}")
    return len(files)


def download_file(adls_path: str, local_path: Path) -> Path:
    """Download one file from ADLS to a local path."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(get_container().download_blob(adls_path).readall())
    return local_path


def download_folder(adls_folder: str = None, local_folder: Path = None) -> Path:
    """Download all files of an ADLS folder; Spark then reads them from the local folder."""
    adls_folder = adls_folder or config.ADLS_FOLDER
    local_folder = Path(local_folder or config.ADLS_DOWNLOAD_DIR)
    for path in list_files(adls_folder):
        download_file(path, local_folder / Path(path).name)
        print(f"downloaded {path}")
    return local_folder


if __name__ == "__main__":
    if test_connection():
        for p in list_files():
            print("  ", p)


                