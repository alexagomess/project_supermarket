import pandas as pd
import unidecode
from scripts.common.etl import BaseETL
from scripts.common.config import FOLDER_RAW, FOLDER_CLEANED_NFE_INFORMATION


class NFEInformationCleaned(BaseETL):
    def __init__(self):
        super().__init__()
        self.folder_raw = FOLDER_RAW
        self.folder_cleaned = FOLDER_CLEANED_NFE_INFORMATION

    def clean_currency(self, value):
        if isinstance(value, str):
            value = value.replace("R$", "").replace(".", "").replace(",", ".").strip()
            return float(value)
        return value

    def main(self):
        raw_files = self.read_google_drive(self.folder_raw)
        shopping_files = [file for file in raw_files if file.endswith("-shopping.csv")]

        for shopping_file in shopping_files:
            self.logger.info(f"Lendo o arquivo: {shopping_file}")
            arquivo = self.read_google_drive(self.folder_raw, shopping_file)

            if arquivo is not None and not arquivo.empty:
                data_dict = {}
                for index, row in arquivo.iterrows():
                    if pd.notnull(row.iloc[1]):
                        data_dict[row.iloc[0]] = row.iloc[1]

                df_info = pd.DataFrame(
                    list(data_dict.items()), columns=["Titulo", "Informacao"]
                )

                df_pivoted = df_info.set_index("Titulo").T

                df_pivoted.columns = [
                    unidecode.unidecode(str(col)).lower() for col in df_pivoted.columns
                ]

                for col in [
                    "valor total do servico",
                    "base de calculo icms",
                    "valor icms",
                ]:
                    if col in df_pivoted.columns:
                        self.logger.info(f"Processando a coluna: {col}")
                        df_pivoted[col] = df_pivoted[col].apply(self.clean_currency)
                    else:
                        self.logger.error(
                            f"Erro: A coluna '{col}' não foi encontrada no arquivo {shopping_file}."
                        )
                        continue

                df_pivoted.dropna(axis=1, how="all", inplace=True)
                df_pivoted.dropna(axis=0, how="all", inplace=True)

                new_file_name = shopping_file.replace("-shopping.csv", "-nfe_info.csv")
                df_pivoted = df_pivoted.dropna(
                    subset=[
                        "nome / razao social",
                        "cnpj",
                        "inscricao estadual",
                        "uf",
                        "destino da operacao",
                        "consumidor final",
                        "presenca do comprador",
                        "modelo",
                        "serie",
                        "numero",
                        "data emissao",
                        "valor total do servico",
                        "base de calculo icms",
                        "valor icms",
                        "protocolo",
                        "chave de acesso",
                    ],
                    how="any",
                )
                if df_pivoted.empty:
                    self.logger.info(
                        "DataFrame está vazio após remoção de linhas nulas. Nenhum arquivo será enviado."
                    )
                    return

                self.logger.info(f"Salvando o arquivo processado: {new_file_name}")
                self.load_google_drive(df_pivoted, new_file_name, self.folder_cleaned)
            else:
                self.logger.error(
                    f"Erro: O arquivo {shopping_file} está vazio ou não foi encontrado."
                )
                continue

    def execute(self):
        try:
            self.main()
            self.logger.info("Processamento concluído com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise e


if __name__ == "__main__":
    etl = NFEInformationCleaned()
    etl.execute()
