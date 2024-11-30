from scripts.docs.oath_gdrive import authenticate
from scripts.common.logging import Logger
from googleapiclient.http import MediaIoBaseUpload
import io


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
