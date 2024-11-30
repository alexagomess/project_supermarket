from scripts.docs.oath_gdrive import authenticate
from io import BytesIO
import pandas as pd
from scripts.common.logging import Logger


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
