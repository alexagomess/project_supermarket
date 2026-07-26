import io
import pandas as pd
import polars as pl
from io import BytesIO
from datetime import datetime
from sqlalchemy import create_engine, text
from hashlib import sha256
from googleapiclient.http import MediaIoBaseUpload
from scripts.common.config import database_url
from scripts.common.polars_mixin import PolarsMixin
from typing import List
from scripts.docs.oath_gdrive import authenticate
from scripts.common.logging import Logger


class BaseETL(PolarsMixin):
    def __init__(self):
        self.text = text
        self.logger = Logger()
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            if database_url is None:
                raise ValueError("database_url não pode ser None.")
            self._engine = create_engine(database_url)
        return self._engine

    def _drive_service(self):
        creds, service = authenticate()
        if service is None:
            raise RuntimeError(
                "Falha na autenticação do Google Drive. Verifique se o token.json "
                "é válido (pode estar expirado/revogado) e o client_secrets.json."
            )
        return service

    def _find_file_id(self, service, folder_id: str, file_name: str):
        query = (
            f"name='{file_name}' and '{folder_id}' in parents "
            "and mimeType='text/csv'"
        )
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get("files", [])
        return items[0]["id"] if items else None

    def read_google_drive(self, folder_id: str, file_name=None):
        service = self._drive_service()

        if not file_name:
            results = (
                service.files()
                .list(
                    q=f"'{folder_id}' in parents",
                    fields="nextPageToken, files(id, name)",
                )
                .execute()
            )
            items = results.get("files", [])
            if not items:
                self.logger.error("Nenhum arquivo encontrado.")
                return None
            return [item["name"] for item in items]

        file_id = self._find_file_id(service, folder_id, file_name)
        if file_id is None:
            self.logger.error("Nenhum arquivo encontrado.")
            return None
        self.logger.info(f"Arquivo encontrado: {file_name} ({file_id})")
        file_data = service.files().get_media(fileId=file_id).execute()
        return pd.read_csv(BytesIO(file_data))

    def read_drive_csv_polars(self, folder_id: str, file_name: str):
        """Lê um CSV do Google Drive como DataFrame Polars (todas as colunas
        como texto, para o parsing controlar os casts)."""
        service = self._drive_service()
        file_id = self._find_file_id(service, folder_id, file_name)
        if file_id is None:
            self.logger.error(f"Arquivo {file_name} não encontrado.")
            return None
        self.logger.info(f"Arquivo encontrado: {file_name} ({file_id})")
        file_data = service.files().get_media(fileId=file_id).execute()
        return pl.read_csv(BytesIO(file_data), infer_schema_length=0)

    def load_google_drive(self, df, file_name, folder_id):
        service = self._drive_service()

        buffer = io.BytesIO()
        df.to_csv(buffer, index=False, encoding="utf-8")
        buffer.seek(0)

        query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get("files", [])

        file_metadata = {"name": file_name, "parents": [folder_id]}

        if items:
            file_id = items[0]["id"]
            media = MediaIoBaseUpload(buffer, mimetype="text/csv", resumable=True)
            service.files().update(fileId=file_id, media_body=media).execute()
            self.logger.info(f"Arquivo atualizado com ID: {file_id}")
        else:
            media = MediaIoBaseUpload(buffer, mimetype="text/csv", resumable=True)
            file = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            self.logger.info(f'Arquivo salvo com ID: {file.get("id")}')

    def save_to_postgres(self, df, table_name, schema="supermarket"):
        """
        Salva o dataframe no postgreSQL
        """
        if database_url is None:
            self.logger.error("database_url está None. Verifique a configuração.")
            raise ValueError("database_url não pode ser None.")
        engine = create_engine(database_url)
        try:
            df.to_sql(
                table_name, engine, schema=schema, if_exists="append", index=False
            )
            self.logger.info(f"DataFrame salvo na tabela '{table_name}' com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao salvar DataFrame na tabela '{table_name}': {e}")
            raise e

    def create_hash(self, df, columns: List[str]):
        """
        Cria uma coluna com o hash dos valores de outras colunas.

        Args:
            df (pd.DataFrame): O DataFrame que contém os dados.
            columns (list): Lista de nomes das colunas a serem usadas para gerar o hash.

        Returns:
            pd.DataFrame: DataFrame com uma nova coluna 'uid' contendo os hashes gerados.
        """
        df["uid"] = df.apply(
            lambda x: sha256(
                "_".join([str(x[col]) for col in columns]).encode("utf-8")
            ).hexdigest(),
            axis=1,
        )
        return df

    def preprocess_dates(self, df, date_columns):
        """
        Converte colunas de data para o formato ISO-8601 (YYYY-MM-DD HH:MM:SS).
        """
        for column in date_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], format="%d/%m/%Y %H:%M:%S", errors="coerce"
                )
                if df[column].isnull().any():
                    self.logger.warning(
                        f"Algumas datas na coluna '{column}' não puderam ser convertidas e foram substituídas por NaT."
                    )
        return df

    def upsert_postgres(
        self,
        df: pl.DataFrame,
        table_name: str,
        columns: List[str],
        conflict_columns: List[str],
        update_columns: List[str],
    ):
        """
        Insere/atualiza dados no PostgreSQL via INSERT ... ON CONFLICT (UPSERT) em lote.

        Args:
            df: DataFrame Polars com os dados.
            table_name: Nome da tabela de destino.
            columns: Colunas a serem inseridas (na ordem desejada).
            conflict_columns: Colunas que compõem a chave de conflito.
            update_columns: Colunas atualizadas quando o registro já existe.
        """
        if df.is_empty():
            self.logger.info("DataFrame vazio; nenhum registro será inserido.")
            return

        key = conflict_columns[0]
        if key in df.columns:
            dups = df.height - df.unique(subset=[key]).height
            if dups > 0:
                self.logger.info(
                    f"Existem {dups} valores duplicados de '{key}' no DataFrame."
                )

        if "updated_at" in df.columns:
            df = df.with_columns(pl.lit(datetime.now()).alias("updated_at"))

        cols_sql = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(f":{c}" for c in columns)
        conflict_sql = ", ".join(f'"{c}"' for c in conflict_columns)
        update_sql = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_columns)

        query = self.text(
            f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql};"
        )

        records = df.select(columns).to_dicts()
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(query, records)
        self.logger.info(
            f"{len(records)} registros processados na tabela '{table_name}'."
        )

    def run_delta_trusted(self):
        """Pipeline padrão da camada trusted (Delta -> Delta, incremental).

        Requer que a subclasse defina: source (Delta cleaned), target (Delta
        trusted), watermark (nome do estado), keys (chave do merge) e o método
        `transform`.
        """
        df, new_wm = self.read_incremental(self.source, self.watermark)
        if df.is_empty():
            self.logger.info("Sem novos registros na camada de origem.")
            return
        out = self.transform(df)
        self.upsert_delta(out, self.target, self.keys)
        if new_wm is not None:
            self.set_watermark(self.watermark, new_wm)
        self.logger.info(
            f"{out.height} registro(s) mesclado(s) na Delta trusted ({self.target})."
        )
