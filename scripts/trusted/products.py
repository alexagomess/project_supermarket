import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from config import FOLDER_CLEANED_SHOPPING
from scripts.common.logging import Logger
from config import DATABASE_URI
from scripts.common.etl import create_hash, read_google_drive


class TrustedProducts:
    def __init__(self):
        self.engine = create_engine(DATABASE_URI)
        self.logger = Logger()
        self.folder = FOLDER_CLEANED_SHOPPING
        self.table_name = "products"
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

        df = df.drop(
            columns=["quantidade", "unidade", "valor_unitario", "reference_date"]
        )
        df = df.drop_duplicates()
        df = create_hash(df, ["codigo", "descricao"])
        df["descricao_completa"] = None
        df["marca"] = None
        df["categoria"] = None
        df["sub_categoria"] = None
        df["ean"] = None
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
        if "uid" in df.columns and df["uid"].duplicated().any():
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
                        INSERT INTO supermarket.{self.table_name} (uid, codigo, descricao, descricao_completa, marca, categoria, sub_categoria, ean, created_at, updated_at)
                        VALUES (
                            :uid,
                            :codigo,
                            :descricao,
                            :descricao_completa,
                            :marca,
                            :categoria,
                            :sub_categoria,
                            :ean,
                            :created_at,
                            :updated_at
                        )
                        ON CONFLICT (uid)
                        DO UPDATE SET
                            descricao = EXCLUDED.descricao,
                            updated_at = EXCLUDED.updated_at;
                    """
                    )

                    params = {
                        "uid": row["uid"],
                        "codigo": row["codigo"],
                        "descricao": row["descricao"],
                        "descricao_completa": row["descricao_completa"],
                        "marca": row["marca"],
                        "categoria": row["categoria"],
                        "sub_categoria": row["sub_categoria"],
                        "ean": row["ean"],
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                    }

                    result = connection.execute(query, params)
                    self.logger.info(
                        f"Inserção realizada para o UID: {row['uid']}, result: {result.rowcount} linhas afetadas"
                    )
            self.logger.info(f"Registros inseridos com sucesso.")


if __name__ == "__main__":
    TrustedProducts().execute()
