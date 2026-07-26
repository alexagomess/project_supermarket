import os
import fcntl
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from scripts.common.logging import Logger

SCOPES = ["https://www.googleapis.com/auth/drive"]
TOKEN_PATH = os.getenv("GOOGLE_TOKEN_FILE", "token.json")
CLIENT_SECRETS_PATH = os.getenv("GOOGLE_CLIENT_SECRETS_FILE", "client_secrets.json")
LOCK_PATH = TOKEN_PATH + ".lock"


def _load_credentials():
    """Lê/renova as credenciais. Deve ser chamado sob o file lock para que
    processos paralelos vejam sempre o token mais recente e apenas um faça o
    refresh."""
    creds = None
    if os.path.exists(TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        except Exception as e:
            Logger.warning(f"token.json inválido, será regenerado: {e}")
            creds = None

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        if not os.path.exists(CLIENT_SECRETS_PATH):
            raise FileNotFoundError(
                f"Arquivo {CLIENT_SECRETS_PATH} não encontrado. "
                "Por favor, forneça o arquivo de credenciais."
            )
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_PATH, SCOPES)
        creds = flow.run_local_server(
            port=8080, access_type="offline", prompt="consent"
        )

    with open(TOKEN_PATH, "w") as token_file:
        token_file.write(creds.to_json())
    return creds


def authenticate():
    try:
        with open(LOCK_PATH, "w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                creds = _load_credentials()
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
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
