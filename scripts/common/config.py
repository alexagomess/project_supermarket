from dotenv import load_dotenv
import os

load_dotenv(override=True)

FOLDER_RAW = os.getenv("FOLDER_RAW")
FOLDER_CLEANED = os.getenv("FOLDER_CLEANED")
FOLDER_CLEANED_SHOPPING = os.getenv("FOLDER_CLEANED_SHOPPING")
FOLDER_CLEANED_NFE_INFORMATION = os.getenv("FOLDER_CLEANED_NFE_INFORMATION")
FOLDER_TRUSTED = os.getenv("FOLDER_TRUSTED")
FOLDER_TRUSTED_SHOPPING = os.getenv("FOLDER_TRUSTED_SHOPPING")
FOLDER_TRUSTED_NFE_INFORMATION = os.getenv("FOLDER_TRUSTED_NFE_INFORMATION")
FOLDER_TRUSTED_PRODUCTS = os.getenv("FOLDER_TRUSTED_PRODUCTS")
FOLDER_TRUSTED_MARKET = os.getenv("FOLDER_TRUSTED_MARKET")

db_user = os.getenv("DB_USER", "postgres")
db_pass = os.getenv("DB_PASS", "postgres")
db_host = os.getenv("DB_HOST", "postgres-app")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME", "supermarket")

database_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

DELTA_ROOT = os.getenv("DELTA_ROOT", "data/delta")
