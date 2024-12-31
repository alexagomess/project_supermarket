import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from config import FOLDER_CLEANED_NFE_INFORMATION, database_url, localhost_url
from scripts.common.logging import Logger
from scripts.common.etl import read_google_drive, preprocess_dates


class TrustedNFEInformation:
    def __init__(self):
        self.engine = create_engine(localhost_url)
        self.logger = Logger()
        self.folder = FOLDER_CLEANED_NFE_INFORMATION
        self.file_cleaned = "nfe_info"
        self.table_name = "nfe_information"

    def execute(self):
        try:
            df = read_google_drive(self.folder)
            df = [f for f in df if f.endswith(f"-{self.file_cleaned}.csv")]
            if not df:
                self.logger.error(
                    f"Nenhum arquivo {self.file_cleaned} encontrado na pasta cleaned."
                )
                return

            for file_name in df:
                self.logger.info(f"Lendo o arquivo: {file_name}")

                file_data = read_google_drive(self.folder, file_name)
                if file_data is not None:
                    df = pd.DataFrame(file_data)
                    self.load_postgres(self.transform(df))
                    self.logger.info(
                        f"Arquivo de {self.file_cleaned} salvo com sucesso."
                    )
                else:
                    self.logger.error(
                        f"Erro ao carregar os dados do arquivo: {file_name}"
                    )
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise e

    def transform(self, df: pd.DataFrame):

        df = df.rename(columns={"nome / razao social": "nome"})
        df = df.rename(columns={"inscricao estadual": "inscricao_estadual"})

        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("-", "_")

        df = df.drop_duplicates()
        df["created_at"] = pd.to_datetime("now")
        df["updated_at"] = pd.to_datetime("now")

        df["nome"] = (
            df["nome"]
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.upper()
        )
        df["uf"] = (
            df["uf"]
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.upper()
        )
        df["destino_da_operacao"] = (
            df["destino_da_operacao"]
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.upper()
        )
        df["consumidor_final"] = (
            df["consumidor_final"]
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.upper()
        )
        df["presenca_do_comprador"] = (
            df["presenca_do_comprador"]
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.upper()
        )

        return df

    def load_postgres(self, df: pd.DataFrame):
        """
        Salva os dados no PostgreSQL usando MERGE/UPSERT para evitar duplicados.

        Args:
            df (pd.DataFrame): O DataFrame que contém os dados a serem salvos.

        Returns:
            None
        """
        date_columns = ["data_emissao", "created_at", "updated_at"]
        df = preprocess_dates(df, date_columns)

        if "chave_de_acesso" in df.columns and df["chave_de_acesso"].duplicated().any():
            self.logger.info("Existem chave_de_acesso duplicados no DataFrame!")
            duplicates = df[df["chave_de_acesso"].duplicated(keep=False)]
            self.logger.info(duplicates)
        else:
            self.logger.info("Nenhuma duplicidade no DataFrame.")
        with self.engine.connect() as connection:
            with connection.begin():
                for _, row in df.iterrows():
                    row["updated_at"] = pd.Timestamp(datetime.now())
                    query = text(
                        f"""
                        INSERT INTO {self.table_name} (nome, cnpj, inscricao_estadual, uf, destino_da_operacao, consumidor_final, 
                        presenca_do_comprador, modelo, serie, numero, data_emissao, valor_total_do_servico, base_de_calculo_icms, valor_icms, protocolo,
                        chave_de_acesso, created_at, updated_at)
                        VALUES (
                            :nome,
                            :cnpj,
                            :inscricao_estadual,
                            :uf,
                            :destino_da_operacao,
                            :consumidor_final,
                            :presenca_do_comprador,
                            :modelo,
                            :serie,
                            :numero,
                            :data_emissao,
                            :valor_total_do_servico,
                            :base_de_calculo_icms,
                            :valor_icms,
                            :protocolo,
                            :chave_de_acesso,
                            :created_at,
                            :updated_at
                        )
                        ON CONFLICT (chave_de_acesso)
                        DO UPDATE SET
                            updated_at = EXCLUDED.updated_at;
                    """
                    )

                    params = {
                        "nome": row["nome"],
                        "cnpj": row["cnpj"],
                        "inscricao_estadual": row["inscricao_estadual"],
                        "uf": row["uf"],
                        "destino_da_operacao": row["destino_da_operacao"],
                        "consumidor_final": row["consumidor_final"],
                        "presenca_do_comprador": row["presenca_do_comprador"],
                        "modelo": row["modelo"],
                        "serie": row["serie"],
                        "numero": row["numero"],
                        "data_emissao": row["data_emissao"],
                        "valor_total_do_servico": row["valor_total_do_servico"],
                        "base_de_calculo_icms": row["base_de_calculo_icms"],
                        "valor_icms": row["valor_icms"],
                        "protocolo": row["protocolo"],
                        "chave_de_acesso": row["chave_de_acesso"],
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                    }

                    result = connection.execute(query, params)
                    self.logger.info(
                        f"Inserção realizada para chave_de_acesso: {row['chave_de_acesso']}, result: {result.rowcount} linhas afetadas"
                    )
            self.logger.info(f"Registros inseridos com sucesso.")


if __name__ == "__main__":
    TrustedNFEInformation().execute()
