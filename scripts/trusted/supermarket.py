import pandas as pd
from datetime import datetime
from scripts.common.etl import read_google_drive, load_google_drive
from config import (
    FOLDER_CLEANED_SHOPPING,
    FOLDER_TRUSTED_SHOPPING,
    FOLDER_CLEANED_SHOPPING,
    FOLDER_TRUSTED_PRODUCTS,
    FOLDER_CLEANED_NFE_INFORMATION,
    FOLDER_TRUSTED_NFE_INFORMATION,
    FOLDER_CLEANED_NFE_INFORMATION,
    FOLDER_TRUSTED_MARKET,
)
from scripts.common.logging import Logger


def save_shopping():
    print("\n")
    Logger.info("Salvando arquivos de shopping...")
    folder_cleaned = FOLDER_CLEANED_SHOPPING
    folder_trusted = FOLDER_TRUSTED_SHOPPING

    # Lê o arquivo de shopping da pasta cleaned
    cleaned_files = read_google_drive(folder_cleaned)
    for cleaned_file in cleaned_files:
        if cleaned_file.endswith("-shopping.csv"):
            df_shopping = read_google_drive(folder_cleaned, cleaned_file)

            # Obtém a data de emissão para o nome do arquivo
            if not df_shopping.empty:
                emission_date = df_shopping["reference_date"].iloc[0]
                formatted_date = pd.to_datetime(emission_date).strftime(
                    "%Y-%m-%d-%H-%M-%S"
                )
                file_name = f"{formatted_date}_shopping.csv"

                load_google_drive(df_shopping, file_name, folder_trusted)
                Logger.info(f"Arquivo '{file_name}' salvo com sucesso.")
    return


def save_products():
    print("\n")
    Logger.info("Salvando arquivos de produtos...")
    folder_cleaned = FOLDER_CLEANED_SHOPPING
    folder_trusted = FOLDER_TRUSTED_PRODUCTS

    # Lê o arquivo de shopping da pasta cleaned
    cleaned_files = read_google_drive(folder_cleaned)
    for cleaned_file in cleaned_files:
        if cleaned_file.endswith("-shopping.csv"):
            df_shopping = read_google_drive(folder_cleaned, cleaned_file)

            # Cria tabela de produtos
            if not df_shopping.empty:
                df_produtos = df_shopping[["descricao", "codigo"]].drop_duplicates()
                df_produtos["ean"] = None  # Adiciona coluna EAN (vazia por enquanto)
                df_produtos["created_at"] = datetime.now()
                df_produtos["updated_at"] = datetime.now()

                # Obtém a data de emissão para o nome do arquivo
                emission_date = df_shopping["reference_date"].iloc[0]
                formatted_date = pd.to_datetime(emission_date).strftime(
                    "%Y-%m-%d-%H-%M-%S"
                )
                file_name = f"{formatted_date}_products.csv"

                # Salva o arquivo de produtos na camada trusted
                if not df_produtos.empty:
                    load_google_drive(df_produtos, file_name, folder_trusted)
                    Logger.info(f"Arquivo '{file_name}' salvo com sucesso.")
    return


def save_nfe_info():
    print("\n")
    Logger.info("Salvando arquivos de informações de NF-e...")
    folder_cleaned = FOLDER_CLEANED_NFE_INFORMATION  # Pasta onde os arquivos são salvos
    folder_trusted = FOLDER_TRUSTED_NFE_INFORMATION  # Pasta onde os arquivos da camada trusted serão salvos

    # Lê o arquivo de nfe_info da pasta cleaned
    cleaned_files = read_google_drive(folder_cleaned)
    for cleaned_file in cleaned_files:
        if cleaned_file.endswith("-nfe_info.csv"):
            df_nfe_info = read_google_drive(folder_cleaned, cleaned_file)

            # Obtém a data de emissão para o nome do arquivo
            if not df_nfe_info.empty:
                emission_date = df_nfe_info["data emissao"].iloc[0]
                formatted_date = pd.to_datetime(emission_date).strftime(
                    "%Y-%m-%d-%H-%M-%S"
                )
                file_name = f"{formatted_date}_nfe_info.csv"

                load_google_drive(df_nfe_info, file_name, folder_trusted)
                Logger.info(f"Arquivo '{file_name}' salvo com sucesso.")
    return


def save_market():
    print("\n")
    Logger.info("Salvando arquivos de mercado...")
    folder_cleaned = FOLDER_CLEANED_NFE_INFORMATION
    folder_trusted = FOLDER_TRUSTED_MARKET

    # Lê o arquivo de nfe_info da pasta cleaned
    cleaned_files = read_google_drive(folder_cleaned)
    for cleaned_file in cleaned_files:
        if cleaned_file.endswith("-nfe_info.csv"):
            df_nfe_info = read_google_drive(folder_cleaned, cleaned_file)

            # Cria tabela de mercado
            if not df_nfe_info.empty:
                df_mercado = df_nfe_info[
                    ["nome / razao social", "cnpj", "inscricao estadual", "uf"]
                ].copy()
                df_mercado.columns = [
                    "nome mercado",
                    "cnpj",
                    "inscricao estadual",
                    "uf",
                ]  # Renomeia as colunas
                df_mercado["created_at"] = datetime.now()
                df_mercado["updated_at"] = datetime.now()

                # Obtém a data de emissão para o nome do arquivo
                emission_date = df_nfe_info["data emissao"].iloc[0]
                formatted_date = pd.to_datetime(emission_date).strftime(
                    "%Y-%m-%d-%H-%M-%S"
                )
                file_name = f"{formatted_date}_market.csv"

                # Salva o arquivo de mercado na camada trusted
                if not df_mercado.empty:
                    load_google_drive(df_mercado, file_name, folder_trusted)
                    Logger.info(f"Arquivo '{file_name}' salvo com sucesso.")
    return


def execute():
    save_shopping()
    save_products()
    save_nfe_info()
    save_market()
    return


if __name__ == "__main__":
    execute()