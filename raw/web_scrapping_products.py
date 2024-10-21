import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from docs.gdrive_write import write_gdrive

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}
today = datetime.today().strftime('%Y_%m_%d')

nfe_key = '31241004737552002262651090001641671801665264'
url_base = 'https://portalsped.fazenda.mg.gov.br/portalnfce/sistema/qrcode.xhtml?p='

# Fazendo a requisição para a página
page = requests.get(f'{url_base}{nfe_key}%7C2%7C1%7C1%7C8B75E1F34CEEB8DE8D620838EBB3E1E845379697', headers=headers)

# Parsing do conteúdo HTML
soup = BeautifulSoup(page.content, 'html.parser')

# Encontrando a tabela específica com as classes 'table' e 'table-striped'
table = soup.find('table', class_='table table-striped')

# Verificando se a tabela foi encontrada
if table:
    # Extraindo os dados da tabela
    table_data = []
    for row in table.find_all('tr'):
        cols = row.find_all('td')
        cols = [ele.text.replace('\n', '').replace('\t', '').strip() for ele in cols]
        table_data.append(cols)

    # Criando um DataFrame com os dados extraídos
    df = pd.DataFrame(table_data)

    # Salvando o DataFrame em um arquivo CSV
    df.to_csv(f'raw/files/{today}_products.csv', index=False, header=False)
    print(f"Dados salvos em '{today}_products.csv'.")
else:
    print("Tabela não encontrada.")