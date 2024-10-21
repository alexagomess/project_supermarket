from autentication import authenticate

# Autenticar e obter as credenciais
creds, service = authenticate()

folder_project = '1PI3lv0xhsQMkV3WWBEIEDMqBrfH5r4YH'
folder_raw = '1k6Kz-Q2Uy4iAGlM_XrSpVgDE4SRDwSju'
folder_cleaned = '1zZkHkdPQynXeZH_T_nnVhHfTqktuZB-X'

# Lista os arquivos na pasta específica
def read_gdrive(service, folder_id):
    results = service.files().list(
        q=f"'{folder_id}' in parents",
        pageSize=10,
        fields="nextPageToken, files(id, name)"
    ).execute()

    items = results.get('files', [])

    if not items:
        print('Nenhum arquivo encontrado.')
    else:
        print('Arquivos:')
        for item in items:
            print(f"{item['name']} ({item['id']})")
    
    return


# Exemplo de uso
folder_project = '1PI3lv0xhsQMkV3WWBEIEDMqBrfH5r4YH'
read_gdrive(service, folder_raw)
