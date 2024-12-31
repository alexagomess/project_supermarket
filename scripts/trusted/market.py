import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from config import (
    FOLDER_CLEANED_NFE_INFORMATION,
    database_url,
    localhost_url,
    google_url,
)
from scripts.common.logging import Logger
from scripts.common.etl import read_google_drive


class TrustedMarket:
    def __init__(self):
        self.engine = create_engine(localhost_url)
        self.logger = Logger()
        self.folder = FOLDER_CLEANED_NFE_INFORMATION
        self.table_name = "market"
        self.file_cleaned = "nfe_info"

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

        df = df[["nome", "cnpj", "inscricao_estadual", "uf"]]
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

        return df

    def load_postgres(self, df: pd.DataFrame):
        """
        Salva os dados no PostgreSQL usando MERGE/UPSERT para evitar duplicados.

        Args:
            df (pd.DataFrame): O DataFrame que contém os dados a serem salvos.

        Returns:
            None
        """
        if "cnpj" in df.columns and df["cnpj"].duplicated().any():
            self.logger.info("Existem cnpj duplicados no DataFrame!")
            duplicates = df[df["cnpj"].duplicated(keep=False)]
            self.logger.info(duplicates)
        else:
            self.logger.info("Nenhuma duplicidade no DataFrame.")
        with self.engine.connect() as connection:
            with connection.begin():
                for _, row in df.iterrows():
                    row["updated_at"] = pd.Timestamp(datetime.now())
                    query = text(
                        f"""
                        INSERT INTO {self.table_name} (nome, cnpj, inscricao_estadual, uf, created_at, updated_at)
                        VALUES (
                            :nome,
                            :cnpj,
                            :inscricao_estadual,
                            :uf,
                            :created_at,
                            :updated_at
                        )
                        ON CONFLICT (cnpj)
                        DO UPDATE SET
                            updated_at = EXCLUDED.updated_at;
                    """
                    )

                    params = {
                        "nome": row["nome"],
                        "cnpj": row["cnpj"],
                        "inscricao_estadual": row["inscricao_estadual"],
                        "uf": row["uf"],
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                    }

                    result = connection.execute(query, params)
                    self.logger.info(
                        f"Inserção realizada para o CNPJ: {row['cnpj']}, result: {result.rowcount} linhas afetadas"
                    )
            self.logger.info(f"Registros inseridos com sucesso.")


if __name__ == "__main__":
    TrustedMarket().execute()
