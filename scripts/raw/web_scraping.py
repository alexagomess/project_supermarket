import re
import requests
import pandas as pd
from urllib.parse import urlparse, parse_qs, unquote
from bs4 import BeautifulSoup
from scripts.common.etl import BaseETL
from scripts.common.config import FOLDER_RAW


class WebScrapingRaw(BaseETL):
    def __init__(self, url):
        super().__init__()
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

    def execute(self):
        self.scrape_data()
        if self.soup:
            self.extract_products()
            self.extract_nfe_info()
            self.extract_key_access()
            self.union_extracted_data()
            self.load()

    def scrape_data(self):
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.content, "html.parser")
            self.logger.info("Requisição realizada com sucesso.")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erro ao realizar a requisição: {e}")
            self.soup = None

    def extract_products(self):
        if self.soup is None:
            self.logger.error(
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
            self.logger.error("Tabela de produtos não encontrada.")
            self.df_products = pd.DataFrame()

    def extract_nfe_info(self):
        if self.soup is None:
            self.logger.error(
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
            self.logger.error("Div com id='collapse4' não encontrada.")
            self.df_info_nfe = pd.DataFrame()

    def extract_key_access(self):
        if self.soup is None:
            self.logger.error(
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
            self.logger.error("Div com id='collapseTwo' não encontrada.")
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
            self.load_google_drive(self.df_combined, file_name, FOLDER_RAW)
            self.logger.info(
                f"Dados combinados salvos no Google Drive como '{file_name}'."
            )


FALLBACK_SIGNATURE = "7AC05039150FDAF2129910938C763BE83592D4F6"


def extract_nfe_key(value: str) -> str:
    """Extrai a chave de 44 dígitos a partir de uma URL completa da NF-e ou da própria chave."""
    value = str(value).strip()
    if value.lower().startswith("http"):
        p = parse_qs(urlparse(value).query).get("p", [""])[0]
        value = unquote(p).split("|")[0]
    key = re.sub(r"\D", "", value)
    if len(key) != 44:
        raise ValueError(
            "Não foi possível extrair uma chave válida de 44 dígitos da entrada informada."
        )
    return key


def build_url(value: str) -> str:
    """Retorna a URL para o GET.

    Se `value` já for uma URL completa, ela é usada como está (preservando a
    assinatura da nota). Caso contrário, monta a URL a partir da chave usando a
    assinatura de fallback.
    """
    value = str(value).strip()
    if value.lower().startswith("http"):
        return value
    key = extract_nfe_key(value)
    return (
        "https://portalsped.fazenda.mg.gov.br/portalnfce/sistema/qrcode.xhtml"
        f"?p={key}%7C2%7C1%7C1%7C{FALLBACK_SIGNATURE}"
    )


def run(nfe_input: str):
    key = extract_nfe_key(nfe_input)
    url = build_url(nfe_input)
    scraper = WebScrapingRaw(url)
    scraper.logger.info(f"Iniciando scraping da NF-e (chave: {key}).")
    scraper.execute()


if __name__ == "__main__":
    run(input("Cole a URL da NF-e ou a chave de 44 dígitos: "))
