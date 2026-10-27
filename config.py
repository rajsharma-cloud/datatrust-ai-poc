"""
All settings in one place.
Values come from the .env file (copy .env.example to .env and edit it).
"""
import os
from pathlib import Path
from dotenv import load_dotenv


PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

#  PostgreSQL: ONE database, schemas bronze / silver / gold (+ audit for run logs)
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DATABASE = os.getenv("PG_DATABASE", "datatrust_poc")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "root")

# Where the source CSV files are read from 
# local : data/source_csv folder (no Azure needed)
# adls  : Azure Data Lake Storage Gen2 container (files uploaded with upload_to_adls.py)
SOURCE_MODE = os.getenv("SOURCE_MODE", "local")
LOCAL_SOURCE_DIR = PROJECT_DIR / "data" / "source_csv"
ADLS_DOWNLOAD_DIR = PROJECT_DIR / "data" / "adls_download"      # files pulled from ADLS land here

# ADLS Gen2 (Azure Storage account with hierarchical namespace) 
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME", "datatrustaicontainer")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY", "H+mg2Qn3yJNjam/mDZ+8H3+JMYcEtwtxBklTUOOhuIoeRyqakJk7gojEiWyOQysatnO9VKzj2VSn+AStoNP0vg==")            # option 1: account key
ADLS_CONNECTION_STRING = os.getenv("ADLS_CONNECTION_STRING", "")   # option 2: connection string
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "landing")          # option 3: neither -> az login (Azure AD)
ADLS_FOLDER = os.getenv("ADLS_FOLDER", "raw")



# ---------- Spark writes to PostgreSQL ----------
#python : rows are sent with PostgreSQL COPY from Python (no extra downloads; good for a laptop POC)
#  jdbc   : Spark JDBC writer (needs the PostgreSQL JDBC jar, downloaded from Maven on first run)
WRITE_MODE = os.getenv("WRITE_MODE", "python")

# ---------- Source files ----------
# file  CSV file name
# bronze  : bronze table it lands in
# date_column : daily files are filtered on this column; None = full snapshot loaded every run
SOURCES = [
    {"file": "payment_orders.csv", "bronze": "bronze.payment_orders", "date_column": "business_date"},
    {"file": "payments.csv",       "bronze": "bronze.payments",       "date_column": "business_date"},
    {"file": "refunds.csv",        "bronze": "bronze.refunds",        "date_column": "business_date"},
    {"file": "settlements.csv",    "bronze": "bronze.settlements",    "date_column": "business_date"},
    {"file": "customers.csv",      "bronze": "bronze.customers",      "date_column": "business_date"},
    {"file": "products.csv",       "bronze": "bronze.products",       "date_column": None},
    {"file": "categories.csv",     "bronze": "bronze.categories",     "date_column": None},
]


