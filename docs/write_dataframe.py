from googleapiclient.http import MediaIoBaseUpload
from docs.oath_gdrive import authenticate
import pandas as pd
import io


def write_df_to_gdrive(df, file_name, folder_id):
    # Autenticar e obter as credenciais e o serviço
    creds, service = authenticate()

    # Criar um objeto BytesIO para armazenar o DataFrame
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False, encoding="utf-8")
    buffer.seek(0)  # Rewind the buffer to the beginning

    # Pesquisar arquivos com o mesmo nome na pasta
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    items = results.get("files", [])

    # Definir os metadados do arquivo
    file_metadata = {"name": file_name, "parents": [folder_id]}

    if items:
        # Se o arquivo já existe, atualiza-o
        file_id = items[0]["id"]
        media = MediaIoBaseUpload(buffer, mimetype="text/csv", resumable=True)
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"Arquivo atualizado com ID: {file_id}")
    else:
        # Se o arquivo não existe, cria um novo
        media = MediaIoBaseUpload(buffer, mimetype="text/csv", resumable=True)
        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        print(f'Arquivo salvo com ID: {file.get("id")}')
