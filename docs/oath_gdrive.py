import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Definir o escopo de permissões
SCOPES = ['https://www.googleapis.com/auth/drive']  # Permissão total ao Google Drive


def authenticate():
    creds = None
    # Verifica se o arquivo token.json existe
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # Se não houver credenciais ou se elas forem inválidas
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Atualiza as credenciais expiradas
            creds.refresh(Request())
        else:
            # Inicia o fluxo de autenticação
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json', SCOPES)
            creds = flow.run_local_server(port=8080)
        
        # Salva as credenciais no arquivo token.json para futuros logins
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    # Cria o serviço do Google Drive
    service = build('drive', 'v3', credentials=creds)

    return creds, service  # Retorna as credenciais e o serviço

# Autenticar e obter as credenciais
if __name__ == "__main__":
    creds, service = authenticate()
