from dotenv import load_dotenv
import os

load_dotenv()

FOLDER_RAW = os.getenv("FOLDER_RAW")
FOLDER_CLEANED = os.getenv("FOLDER_CLEANED")
FOLDER_CLEANED_SHOPPING = os.getenv("FOLDER_CLEANED_SHOPPING")
FOLDER_CLEANED_NFE_INFORMATION = os.getenv("FOLDER_CLEANED_NFE_INFORMATION")
FOLDER_TRUSTED = os.getenv("FOLDER_TRUSTED")
FOLDER_TRUSTED_SHOPPING = os.getenv("FOLDER_TRUSTED_SHOPPING")
FOLDER_TRUSTED_NFE_INFORMATION = os.getenv("FOLDER_TRUSTED_NFE_INFORMATION")
FOLDER_TRUSTED_PRODUCTS = os.getenv("FOLDER_TRUSTED_PRODUCTS")
FOLDER_TRUSTED_MARKET = os.getenv("FOLDER_TRUSTED_MARKET")

db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_host = os.getenv("INSTANCE_HOST")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME")
localhost_url = os.getenv("LOCALHOST_URL")
google_host = os.getenv("GOOGLE_HOST")

database_url = f"postgresql+pg8000://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
google_url = f"postgresql://{db_user}:{db_pass}@{google_host}:{db_port}/{db_name}"
