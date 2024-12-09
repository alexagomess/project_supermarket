import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from config import FOLDER_CLEANED_SHOPPING
from scripts.common.logging import Logger
from config import DATABASE_URI
from scripts.common.etl import create_hash, read_google_drive


class TrustedShopping:
    def __init__(self):
        self.engine = create_engine(DATABASE_URI)
        self.logger = Logger()
        self.table_name = "shopping"
        self.folder = FOLDER_CLEANED_SHOPPING
        self.file_cleaned = "shopping"

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
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("-", "_")
        df["index"] = df.index
        df = create_hash(df, ["index", "codigo", "descricao", "reference_date", "chave_de_acesso"])
        df["created_at"] = pd.to_datetime("now")
        df["updated_at"] = pd.to_datetime("now")
        return df

    def load_postgres(self, df: pd.DataFrame):
        """
        Salva os dados no PostgreSQL usando MERGE/UPSERT para evitar duplicados.

        Args:
            df (pd.DataFrame): O DataFrame que contém os dados a serem salvos.

        Returns:
            None
        """
        if df["uid"].duplicated().any():
            self.logger.info("Existem duplicados no DataFrame!")
            duplicates = df[df["uid"].duplicated(keep=False)]
            self.logger.info(duplicates)
        else:
            self.logger.info("Nenhuma duplicidade no DataFrame.")
        with self.engine.connect() as connection:
            with connection.begin():
                for _, row in df.iterrows():
                    row["updated_at"] = pd.Timestamp(datetime.now())
                    query = text(
                        f"""
                        INSERT INTO supermarket.{self.table_name} (uid, "index", codigo, descricao, reference_date, quantidade, unidade, valor_unitario, chave_de_acesso, created_at, updated_at)
                        VALUES (
                            :uid,
                            :index,
                            :codigo,
                            :descricao,
                            :reference_date,
                            :quantidade,
                            :unidade,
                            :valor_unitario,
                            :chave_de_acesso,
                            :created_at,
                            :updated_at
                        )
                        ON CONFLICT (uid)
                        DO UPDATE SET
                            descricao = EXCLUDED.descricao,
                            reference_date = EXCLUDED.reference_date,
                            quantidade = EXCLUDED.quantidade,
                            unidade = EXCLUDED.unidade,
                            valor_unitario = EXCLUDED.valor_unitario,
                            updated_at = EXCLUDED.updated_at;
                    """
                    )

                    params = {
                        "uid": row["uid"],
                        "index": row["index"],
                        "codigo": row["codigo"],
                        "descricao": row["descricao"],
                        "reference_date": row["reference_date"],
                        "quantidade": row["quantidade"],
                        "unidade": row["unidade"],
                        "valor_unitario": row["valor_unitario"],
                        "chave_de_acesso": row["chave_de_acesso"],
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                    }

                    result = connection.execute(query, params)
                    self.logger.info(
                        f"Inserção realizada para o UID: {row['uid']}, result: {result.rowcount} linhas afetadas"
                    )
            self.logger.info(f"Registros inseridos com sucesso.")


if __name__ == "__main__":
    TrustedShopping().execute()
