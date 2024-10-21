from googleapiclient.http import MediaFileUpload
from autentication import authenticate

# Autenticar e obter as credenciais e o serviço
creds, service = authenticate()

# Defina o ID da pasta onde deseja salvar o arquivo
folder_project = '1PI3lv0xhsQMkV3WWBEIEDMqBrfH5r4YH'
folder_raw = '1k6Kz-Q2Uy4iAGlM_XrSpVgDE4SRDwSju'
folder_cleaned = '1zZkHkdPQynXeZH_T_nnVhHfTqktuZB-X'


# Defina o arquivo que deseja enviar
file_metadata = {
    'name': 'nome_do_arquivo.txt',  # Nome do arquivo que será salvo
    'parents': [folder_raw]  # ID da pasta onde o arquivo será salvo
}

# Cria um objeto MediaFileUpload para o arquivo que será enviado
media = MediaFileUpload('raw/files/2024_10_21_products.csv', mimetype='text/plain')  # Substitua pelo caminho do seu arquivo

# Enviar o arquivo
file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
print(f'Arquivo salvo com ID: {file.get("id")}')
