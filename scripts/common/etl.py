from scripts.docs.oath_gdrive import authenticate
from io import BytesIO
import pandas as pd
from scripts.common.logging import Logger
from googleapiclient.http import MediaIoBaseUpload
import io
from sqlalchemy import create_engine, text
from config import localhost_url, database_url
from typing import List
from hashlib import sha256


def read_google_drive(folder_id, file_name=None):
    creds, service = authenticate()

    query = f"'{folder_id}' in parents"
    if file_name:
        query += f" and name='{file_name}' and mimeType='text/csv'"

    results = (
        service.files().list(q=query, fields="nextPageToken, files(id, name)").execute()
    )

    items = results.get("files", [])

    if not items:
        Logger.error("Nenhum arquivo encontrado.")
        return None
    else:
        if file_name:
            file_id = items[0]["id"]
            Logger.info(f"Arquivo encontrado: {items[0]['name']} ({file_id})")

            request = service.files().get_media(fileId=file_id)
            file_data = request.execute()

            df = pd.read_csv(BytesIO(file_data))

            return df

        return [item["name"] for item in items]


def load_google_drive(df, file_name, folder_id):
    creds, service = authenticate()

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
        Logger.info(f"Arquivo atualizado com ID: {file_id}")
    else:
        media = MediaIoBaseUpload(buffer, mimetype="text/csv", resumable=True)
        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        Logger.info(f'Arquivo salvo com ID: {file.get("id")}')


def save_to_postgres(df, table_name, schema="supermarket"):
    """
    Salva o dataframe no postgreSQL
    """
    engine = create_engine(localhost_url)
    try:
        df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
        Logger.info(f"DataFrame salvo na tabela '{table_name}' com sucesso.")
    except Exception as e:
        Logger.error(f"Erro ao salvar DataFrame na tabela '{table_name}': {e}")
        raise e


def create_hash(df, columns: List[str]):
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


def preprocess_dates(df, date_columns):
    """
    Converte colunas de data para o formato ISO-8601 (YYYY-MM-DD HH:MM:SS).
    """
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column], format="%d/%m/%Y %H:%M:%S", errors="coerce"
            )
            if df[column].isnull().any():
                Logger.warning(
                    f"Algumas datas na coluna '{column}' não puderam ser convertidas e foram substituídas por NaT."
                )
    return df
