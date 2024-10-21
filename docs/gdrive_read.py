from docs.oath_gdrive import authenticate
from googleapiclient.discovery import build
from io import BytesIO
import pandas as pd

def read_gdrive(folder_id, file_name):
    # Autenticar e obter as credenciais e o serviço
    creds, service = authenticate()

    # Lista os arquivos na pasta específica
    results = service.files().list(
        q=f"'{folder_id}' in parents and name='{file_name}' and mimeType='text/csv'",  # Filtra pelo nome do arquivo .csv na pasta especificada
        pageSize=1,  # Queremos apenas um arquivo
        fields="nextPageToken, files(id, name)"
    ).execute()

    items = results.get('files', [])

    if not items:
        print('Nenhum arquivo encontrado.')
        return None
    else:
        # Obtém o ID do arquivo encontrado
        file_id = items[0]['id']
        print(f"Arquivo encontrado: {items[0]['name']} ({file_id})")

        # Faz o download do arquivo CSV
        request = service.files().get_media(fileId=file_id)
        file_data = request.execute()  # Executa o download

        # Cria um DataFrame a partir dos bytes do arquivo CSV
        df = pd.read_csv(BytesIO(file_data))  # Lê o CSV em um DataFrame

        return df  # Retorna o DataFrame
