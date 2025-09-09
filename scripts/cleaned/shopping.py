import pandas as pd
from datetime import datetime
from scripts.common.etl import BaseETL
from scripts.common.config import FOLDER_RAW, FOLDER_CLEANED_SHOPPING


class ShoppingCleaned(BaseETL):
    def __init__(self):
        super().__init__()
        self.folder_raw = FOLDER_RAW
        self.folder_cleaned = FOLDER_CLEANED_SHOPPING

    def main(self):
        raw_files = self.read_google_drive(self.folder_raw)

        shopping_files = [file for file in raw_files if file.endswith("-shopping.csv")]

        for shopping_file in shopping_files:
            arquivo = self.read_google_drive(self.folder_raw, shopping_file)

            if arquivo is not None and not arquivo.empty:
                emission_date: str = None
                key_nfe: str = None
                for index, row in arquivo.iterrows():
                    row = row.astype(str)
                    if "Data Emissão" in row.iloc[0]:
                        emission_date = row.iloc[1]
                    elif "Chave de Acesso" in row.iloc[0]:
                        key_nfe = row.iloc[1]

                if emission_date is not None:
                    reference_date = datetime.strptime(
                        emission_date, "%d/%m/%Y %H:%M:%S"
                    )

                    arquivo.replace(r'["“”]', "", regex=True, inplace=True)

                    split_columns = arquivo.iloc[:, 2].str.split(
                        r"\(Código:", expand=True
                    )

                    if split_columns.shape[1] < 2:
                        self.logger.error(
                            f"Erro: A divisão da coluna não gerou duas partes para o arquivo {shopping_file}."
                        )
                        continue

                    descricao = (
                        split_columns[0]
                        .str.replace(r'["“”"]', "", regex=True)
                        .replace(r",", " ", regex=True)
                        .str.strip()
                        .str.upper()
                    )
                    código = split_columns[1].str.strip(")").astype(str)

                    quantidade = (
                        arquivo.iloc[:, 3]
                        .str.strip()
                        .replace(r"[^\d.]", "", regex=True)
                        .astype(float)
                    )
                    unidade = (
                        arquivo.iloc[:, 4]
                        .replace(r".*:", "", regex=True)
                        .str.strip()
                        .str.upper()
                    )
                    valor_unitario = (
                        arquivo.iloc[:, 5]
                        .str.strip()
                        .replace(r"[^\d,]", "", regex=True)
                        .str.replace(",", ".")
                        .str.strip()
                        .astype(float)
                    )

                    created_at = datetime.now()
                    updated_at = datetime.now()

                    data_dict = {
                        "chave_de_acesso": key_nfe,
                        "descricao": descricao,
                        "codigo": código,
                        "quantidade": quantidade,
                        "unidade": unidade,
                        "valor unitario": valor_unitario,
                        "reference_date": reference_date,
                        "created_at": created_at,
                        "updated_at": updated_at,
                    }

                    df = pd.DataFrame(data_dict)
                    df = df.dropna(
                        subset=[
                            "chave_de_acesso",
                            "descricao",
                            "codigo",
                            "quantidade",
                            "unidade",
                            "valor unitario",
                        ],
                        how="any",
                    )
                    if df.empty:
                        self.logger.info(
                            "DataFrame está vazio após remoção de linhas nulas. Nenhum arquivo será enviado."
                        )
                        return

                    self.load_google_drive(df, shopping_file, self.folder_cleaned)
                else:
                    self.logger.error(
                        f"O arquivo {shopping_file} não contém a data de emissão."
                    )
                    continue
            else:
                self.logger.error(
                    f"O arquivo {shopping_file} está vazio ou não foi encontrado."
                )
                continue

    def execute(self):
        try:
            self.main()
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise e
        return


if __name__ == "__main__":
    etl = ShoppingCleaned()
    etl.execute()
