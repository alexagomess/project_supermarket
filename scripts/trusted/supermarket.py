import pandas as pd
from datetime import datetime
from scripts.common.etl import BaseETL
from scripts.common.config import (
    FOLDER_CLEANED_SHOPPING,
    FOLDER_TRUSTED_SHOPPING,
    FOLDER_CLEANED_SHOPPING,
    FOLDER_TRUSTED_PRODUCTS,
    FOLDER_CLEANED_NFE_INFORMATION,
    FOLDER_TRUSTED_NFE_INFORMATION,
    FOLDER_CLEANED_NFE_INFORMATION,
    FOLDER_TRUSTED_MARKET,
)


class TrustedSupermarket(BaseETL):
    def __init__(self):
        super().__init__()
        self.folder_cleaned_shopping = FOLDER_CLEANED_SHOPPING
        self.folder_trusted_shopping = FOLDER_TRUSTED_SHOPPING
        self.folder_trusted_products = FOLDER_TRUSTED_PRODUCTS
        self.folder_cleaned_nfe_information = FOLDER_CLEANED_NFE_INFORMATION
        self.folder_trusted_nfe_information = FOLDER_TRUSTED_NFE_INFORMATION
        self.folder_trusted_market = FOLDER_TRUSTED_MARKET

    def save_shopping(self):
        print("\n")
        self.logger.info("Salvando arquivos de shopping...")

        cleaned_files = self.read_google_drive(self.folder_cleaned_shopping)
        for cleaned_file in cleaned_files:
            if cleaned_file.endswith("-shopping.csv"):
                df_shopping = self.read_google_drive(
                    self.folder_cleaned_shopping, cleaned_file
                )

                if not df_shopping.empty:
                    emission_date = df_shopping["reference_date"].iloc[0]
                    formatted_date = pd.to_datetime(emission_date).strftime(
                        "%Y-%m-%d-%H-%M-%S"
                    )
                    file_name = f"{formatted_date}_shopping.csv"

                    self.load_google_drive(
                        df_shopping, file_name, self.folder_trusted_shopping
                    )
                    self.logger.info(f"Arquivo '{file_name}' salvo com sucesso.")
        return

    def save_products(self):
        print("\n")
        self.logger.info("Salvando arquivos de produtos...")

        cleaned_files = self.read_google_drive(self.folder_cleaned_shopping)
        for cleaned_file in cleaned_files:
            if cleaned_file.endswith("-shopping.csv"):
                df_shopping = self.read_google_drive(
                    self.folder_cleaned_shopping, cleaned_file
                )

                if not df_shopping.empty:
                    df_produtos = df_shopping[["descricao", "codigo"]].drop_duplicates()
                    df_produtos["ean"] = None
                    df_produtos["created_at"] = datetime.now()
                    df_produtos["updated_at"] = datetime.now()

                    emission_date = df_shopping["reference_date"].iloc[0]
                    formatted_date = pd.to_datetime(emission_date).strftime(
                        "%Y-%m-%d-%H-%M-%S"
                    )
                    file_name = f"{formatted_date}_products.csv"

                    if not df_produtos.empty:
                        self.load_google_drive(
                            df_produtos, file_name, self.folder_trusted_products
                        )
                        self.logger.info(f"Arquivo '{file_name}' salvo com sucesso.")
        return

    def save_nfe_info(self):
        print("\n")
        self.logger.info("Salvando arquivos de informações de NF-e...")

        cleaned_files = self.read_google_drive(self.folder_cleaned_nfe_information)
        for cleaned_file in cleaned_files:
            if cleaned_file.endswith("-nfe_info.csv"):
                df_nfe_info = self.read_google_drive(
                    self.folder_cleaned_nfe_information, cleaned_file
                )

                if not df_nfe_info.empty:
                    emission_date = df_nfe_info["data emissao"].iloc[0]
                    formatted_date = pd.to_datetime(emission_date).strftime(
                        "%Y-%m-%d-%H-%M-%S"
                    )
                    file_name = f"{formatted_date}_nfe_info.csv"

                    self.load_google_drive(
                        df_nfe_info, file_name, self.folder_trusted_nfe_information
                    )
                    self.logger.info(f"Arquivo '{file_name}' salvo com sucesso.")
        return

    def save_market(self):
        print("\n")
        self.logger.info("Salvando arquivos de mercado...")

        cleaned_files = self.read_google_drive(self.folder_cleaned_nfe_information)
        for cleaned_file in cleaned_files:
            if cleaned_file.endswith("-nfe_info.csv"):
                df_nfe_info = self.read_google_drive(
                    self.folder_cleaned_nfe_information, cleaned_file
                )

                if not df_nfe_info.empty:
                    df_mercado = df_nfe_info[
                        ["nome / razao social", "cnpj", "inscricao estadual", "uf"]
                    ].copy()
                    df_mercado.columns = [
                        "nome mercado",
                        "cnpj",
                        "inscricao estadual",
                        "uf",
                    ]
                    df_mercado["created_at"] = datetime.now()
                    df_mercado["updated_at"] = datetime.now()

                    emission_date = df_nfe_info["data emissao"].iloc[0]
                    formatted_date = pd.to_datetime(emission_date).strftime(
                        "%Y-%m-%d-%H-%M-%S"
                    )
                    file_name = f"{formatted_date}_market.csv"

                    if not df_mercado.empty:
                        self.load_google_drive(
                            df_mercado, file_name, self.folder_trusted_market
                        )
                        self.logger.info(f"Arquivo '{file_name}' salvo com sucesso.")
        return

    def execute(self):
        self.save_shopping()
        self.save_products()
        self.save_nfe_info()
        self.save_market()


if __name__ == "__main__":
    etl = TrustedSupermarket()
    etl.execute()
