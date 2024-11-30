import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from scripts.common.logging import Logger

SCOPES = ["https://www.googleapis.com/auth/drive"]


def authenticate():
    creds = None
    token_path = "token.json"
    credentials_path = "client_secrets.json"

    try:
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(credentials_path):
                    raise FileNotFoundError(
                        f"Arquivo {credentials_path} não encontrado. Por favor, forneça o arquivo de credenciais."
                    )

                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=8080)

            with open(token_path, "w") as token:
                token.write(creds.to_json())

    except Exception as e:
        Logger.error(f"Erro na autenticação: {e}")
        return None, None

    try:
        service = build("drive", "v3", credentials=creds)
    except Exception as e:
        Logger.error(f"Erro ao criar o serviço do Google Drive: {e}")
        return creds, None

    return creds, service


if __name__ == "__main__":
    creds, service = authenticate()
    if creds and service:
        print("Autenticação bem-sucedida e serviço do Google Drive criado.")
    else:
        print("Falha na autenticação ou criação do serviço.")
