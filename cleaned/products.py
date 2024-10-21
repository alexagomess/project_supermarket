from venv import logger
import pandas as pd
from datetime import datetime

today = datetime.today().strftime('%Y_%m_%d')

arquivo = pd.read_csv(f'raw/files/{today}_products.csv', header=None, encoding='utf-8')

arquivo.replace(r'["“”]', '', regex=True, inplace=True)
split_columns = arquivo[0].str.split(r"\(Código:", expand=True)
descrição = split_columns[0].str.replace(r'["“”"]', '', regex=True).replace(r',', ' ', regex=True).str.strip().str.upper()  # Remover aspas antes de outros tratamentos
código = split_columns[1].str.strip(')').astype(int)
quantidade = arquivo[1].str.strip().replace(r'[^\d.]', '', regex=True).astype(float) # O replace remove tudo que não for dígito
unidade = arquivo[2].replace(r'.*:', '', regex=True).str.strip().str.upper() # O replace remove tudo que não for dígito
valor_unitário = arquivo[3].str.strip().replace(r'[^\d,]', '', regex=True).str.replace(',', '.').str.strip().astype(float) # Aplicando a expressão regular para remover todos os caracteres que não sejam dígitos ou vírgulas

dict = {
    'Descricao': descrição,
    'Codigo': código,
    'Quantidade': quantidade,
    'Unidade': unidade,
    'Valor Unitario': valor_unitário
}

df = pd.DataFrame(dict)
df.to_csv(f'cleaned/files/{today}_products.csv', index=False)
print(f'Dados salvos em {today}_products.csv')