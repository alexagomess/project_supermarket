import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from docs.write_dataframe import write_df_to_gdrive

# Definir cabeçalho de requisição e chave da NFe
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}
nfe_key = '31241004737552002262651090001641671801665264'
url_base = 'https://portalsped.fazenda.mg.gov.br/portalnfce/sistema/qrcode.xhtml?p='
url_final = f'{url_base}{nfe_key}%7C2%7C1%7C1%7C8B75E1F34CEEB8DE8D620838EBB3E1E845379697'
today = datetime.today().strftime('%Y_%m_%d')

# Fazendo a requisição para a página
page = requests.get(url_final, headers=headers)
soup = BeautifulSoup(page.content, 'html.parser')

# Extrair dados dos produtos
def extract_products(soup):
    table = soup.find('table', class_='table table-striped')
    if table:
        table_data = []
        for row in table.find_all('tr'):
            cols = row.find_all('td')
            cols = [ele.text.replace('\n', '').replace('\t', '').strip() for ele in cols]
            table_data.append(cols)
        df_products = pd.DataFrame(table_data)
        return df_products
    else:
        print("Tabela de produtos não encontrada.")
        return pd.DataFrame()  # Retorna DataFrame vazio se não encontrar a tabela

# Extrair dados das informações da nota em formato de linha
def extract_nfe_info(soup):
    collapse_div = soup.find('div', id='collapse4')
    data_dict = {}
    if collapse_div:
        for table in collapse_div.find_all('table'):
            headers = table.find_all('th')
            values = table.find_all('td')
            for header, value in zip(headers, values):
                title = header.get_text(strip=True)
                content = value.get_text(strip=True)
                data_dict[title] = content
        
        # Transformando o dicionário em um DataFrame com formato de linha
        df_info_nfe = pd.DataFrame(list(data_dict.items()), columns=["Título", "Valor"])
        return df_info_nfe
    else:
        print("Div com id='collapse4' não encontrada.")
        return pd.DataFrame()  # Retorna DataFrame vazio se não encontrar a div

# Extraindo os DataFrames
df_products = extract_products(soup)
df_info_nfe = extract_nfe_info(soup)

# Unificar ambos os DataFrames em um único DataFrame, se necessário
df_combined = pd.DataFrame()

if not df_products.empty and not df_info_nfe.empty:
    # Unindo os DataFrames horizontalmente
    df_combined = pd.concat([df_info_nfe, df_products], axis=1)
    
    # Extraindo a data da coluna "Data Emissão"
    if 'Data Emissão' in df_info_nfe['Título'].values:
        date_str = df_info_nfe.loc[df_info_nfe['Título'] == 'Data Emissão', 'Valor'].values[0]
        
        # Convertendo o valor de string para um objeto datetime
        date_obj = pd.to_datetime(date_str, format="%d/%m/%Y %H:%M:%S")

        # Formatando a data no formato desejado
        formatted_date = date_obj.strftime("%Y-%m-%d-%H-%M-%S")

        # Criando o nome do arquivo
        file_name = f"{formatted_date}-shopping.csv"
        
        write_df_to_gdrive(df_combined, file_name, '1k6Kz-Q2Uy4iAGlM_XrSpVgDE4SRDwSju')
        print(f"Dados combinados salvos no Google Drive como '{file_name}'.")

# Salvando ambos os DataFrames no Google Drive
if not df_products.empty:
    write_df_to_gdrive(df_products, f"{formatted_date}_products.csv", '1k6Kz-Q2Uy4iAGlM_XrSpVgDE4SRDwSju')
    print("Dados dos produtos salvos no Google Drive.")

if not df_info_nfe.empty:
    write_df_to_gdrive(df_info_nfe, f"{formatted_date}_info_nfe.csv", '1k6Kz-Q2Uy4iAGlM_XrSpVgDE4SRDwSju')
    print("Dados das informações da nota salvos no Google Drive.")

# Exibindo os DataFrames
print("Dados dos produtos:")
print(df_products)
print("\nDados das informações da nota:")
print(df_info_nfe)
print("\nDados combinados:")
print(df_combined)
