import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from scripts.common.logging import Logger

# Definir o escopo de permissões
SCOPES = ["https://www.googleapis.com/auth/drive"]  # Permissão total ao Google Drive


def authenticate():
    creds = None
    token_path = "token.json"
    credentials_path = "client_secrets.json"

    try:
        # Verifica se o arquivo token.json existe
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)

        # Se não houver credenciais ou se elas forem inválidas
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                # Atualiza as credenciais expiradas
                creds.refresh(Request())
            else:
                # Verifica se o arquivo client_secrets.json existe
                if not os.path.exists(credentials_path):
                    raise FileNotFoundError(
                        f"Arquivo {credentials_path} não encontrado. Por favor, forneça o arquivo de credenciais."
                    )

                # Inicia o fluxo de autenticação
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=8080)

            # Salva as credenciais no arquivo token.json para futuros logins
            with open(token_path, "w") as token:
                token.write(creds.to_json())

    except Exception as e:
        Logger.error(f"Erro na autenticação: {e}")
        return None, None

    # Cria o serviço do Google Drive
    try:
        service = build("drive", "v3", credentials=creds)
    except Exception as e:
        Logger.error(f"Erro ao criar o serviço do Google Drive: {e}")
        return creds, None

    return creds, service  # Retorna as credenciais e o serviço


# Autenticar e obter as credenciais
if __name__ == "__main__":
    creds, service = authenticate()
    if creds and service:
        print("Autenticação bem-sucedida e serviço do Google Drive criado.")
    else:
        print("Falha na autenticação ou criação do serviço.")