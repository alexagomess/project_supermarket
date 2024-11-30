import requests
import pandas as pd
from bs4 import BeautifulSoup
from scripts.common.etl import load_google_drive
from config import FOLDER_RAW
from scripts.common.logging import Logger


class WebScraping:
    def __init__(self, url):
        self.url = url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
        }
        self.soup = None
        self.df_products = pd.DataFrame()
        self.df_info_nfe = pd.DataFrame()
        self.df_key_access = pd.DataFrame()
        self.df_combined = pd.DataFrame()
        self.formatted_date = None

    def scrape_data(self):
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.content, "html.parser")
            Logger.info("Requisição realizada com sucesso.")
        except requests.exceptions.RequestException as e:
            Logger.error(f"Erro ao realizar a requisição: {e}")
            self.soup = None

    def extract_products(self):
        if self.soup is None:
            Logger.error(
                "HTML não foi carregado. Certifique-se de executar scrape_data primeiro."
            )
            return
        table = self.soup.find("table", class_="table table-striped")
        if table:
            table_data = [
                [
                    ele.text.replace("\n", "").replace("\t", "").strip()
                    for ele in row.find_all("td")
                ]
                for row in table.find_all("tr")
            ]
            self.df_products = pd.DataFrame(table_data)
        else:
            Logger.error("Tabela de produtos não encontrada.")
            self.df_products = pd.DataFrame()

    def extract_nfe_info(self):
        if self.soup is None:
            Logger.error(
                "HTML não foi carregado. Certifique-se de executar scrape_data primeiro."
            )
            return
        collapse_div = self.soup.find("div", id="collapse4")
        data_dict = {}
        if collapse_div:
            for table in collapse_div.find_all("table"):
                headers = table.find_all("th")
                values = table.find_all("td")
                for header, value in zip(headers, values):
                    title = header.get_text(strip=True)
                    content = value.get_text(strip=True)
                    data_dict[title] = content
            self.df_info_nfe = pd.DataFrame(
                list(data_dict.items()), columns=["Título", "Valor"]
            )
        else:
            Logger.error("Div com id='collapse4' não encontrada.")
            self.df_info_nfe = pd.DataFrame()

    def extract_key_access(self):
        if self.soup is None:
            Logger.error(
                "HTML não foi carregado. Certifique-se de executar scrape_data primeiro."
            )
            return
        collapse_div = self.soup.find("div", id="collapseTwo")
        data_dict = {}
        if collapse_div:
            table = collapse_div.find("table")
            if table:
                key_cell = table.find("td")
                if key_cell:
                    data_dict["Chave de Acesso"] = key_cell.get_text(strip=True)
            self.df_key_access = pd.DataFrame(
                list(data_dict.items()), columns=["Título", "Valor"]
            )
        else:
            Logger.error("Div com id='collapseTwo' não encontrada.")
            self.df_key_access = pd.DataFrame()

    def union_extracted_data(self):
        if not self.df_products.empty and not self.df_info_nfe.empty:
            df_nfe_key = pd.concat(
                [self.df_info_nfe, self.df_key_access], axis=0, ignore_index=True
            )
            self.df_combined = pd.concat([df_nfe_key, self.df_products], axis=1)
            if "Data Emissão" in self.df_info_nfe["Título"].values:
                date_str = self.df_info_nfe.loc[
                    self.df_info_nfe["Título"] == "Data Emissão", "Valor"
                ].values[0]
                date_obj = pd.to_datetime(date_str, format="%d/%m/%Y %H:%M:%S")
                self.formatted_date = date_obj.strftime("%Y-%m-%d-%H-%M-%S")

    def load(self):
        if not self.df_combined.empty:
            file_name = f"{self.formatted_date}-shopping.csv"
            load_google_drive(self.df_combined, file_name, FOLDER_RAW)
            Logger.info(f"Dados combinados salvos no Google Drive como '{file_name}'.")

    def execute(self):
        self.scrape_data()
        if self.soup:
            self.extract_products()
            self.extract_nfe_info()
            self.extract_key_access()
            self.union_extracted_data()
            self.load()


if __name__ == "__main__":
    nfe_key = input("Digite o código da NFe de 44 números: ")
    if len(nfe_key) != 44:
        print("Código inválido! Certifique-se de digitar 44 números.")
    else:
        url = f"https://portalsped.fazenda.mg.gov.br/portalnfce/sistema/qrcode.xhtml?p={nfe_key}%7C2%7C1%7C1%7C8B75E1F34CEEB8DE8D620838EBB3E1E845379697"
        scraper = WebScraping(url)
        scraper.execute()
