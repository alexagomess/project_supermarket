from dotenv import load_dotenv
import os

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Inicializa as variáveis de ambiente
FOLDER_RAW = os.getenv('FOLDER_RAW')
FOLDER_CLEANED = os.getenv('FOLDER_CLEANED')
FOLDER_TRUSTED = os.getenv('FOLDER_TRUSTED')
FOLDER_TRUSTED_SHOPPING = os.getenv('FOLDER_TRUSTED_SHOPPING')
FOLDER_TRUSTED_NFE_INFORMATION = os.getenv('FOLDER_TRUSTED_NFE_INFORMATION')
FOLDER_TRUSTED_PRODUCTS = os.getenv('FOLDER_TRUSTED_PRODUCTS')
FOLDER_TRUSTED_MARKET = os.getenv('FOLDER_TRUSTED_MARKET')