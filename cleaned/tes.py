from docs.gdrive_read import read_gdrive
from docs.autentication import authenticate

# Autenticar e obter as credenciais
creds, service = authenticate()

# Passar o serviço como um parâmetro para a função read_gdrive
folder_raw = '1k6Kz-Q2Uy4iAGlM_XrSpVgDE4SRDwSju'

# Chamar a função para listar os arquivos na pasta específica
read_gdrive(service, folder_raw)
