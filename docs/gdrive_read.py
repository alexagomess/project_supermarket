from docs.oath_gdrive import authenticate
from googleapiclient.discovery import build
from io import BytesIO
import pandas as pd

def read_gdrive(folder_id, file_name=None):
    # Autenticar e obter as credenciais e o serviço
    creds, service = authenticate()

    # Lista os arquivos na pasta específica
    query = f"'{folder_id}' in parents"
    if file_name:
        query += f" and name='{file_name}' and mimeType='text/csv'"  # Filtra pelo nome do arquivo .csv na pasta especificada

    results = service.files().list(
        q=query,
        fields="nextPageToken, files(id, name)"
    ).execute()

    items = results.get('files', [])

    if not items:
        print('Nenhum arquivo encontrado.')
        return None
    else:
        # Se um nome de arquivo específico foi fornecido, retorna o DataFrame
        if file_name:
            file_id = items[0]['id']
            print(f"Arquivo encontrado: {items[0]['name']} ({file_id})")

            # Faz o download do arquivo CSV
            request = service.files().get_media(fileId=file_id)
            file_data = request.execute()  # Executa o download

            # Cria um DataFrame a partir dos bytes do arquivo CSV
            df = pd.read_csv(BytesIO(file_data))  # Lê o CSV em um DataFrame

            return df  # Retorna o DataFrame

        # Se não foi fornecido um nome de arquivo, retorna a lista de arquivos
        return [item['name'] for item in items]  # Retorna uma lista de nomes de arquivos
