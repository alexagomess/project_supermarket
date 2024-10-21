import pandas as pd
from datetime import datetime
from docs.gdrive_read import read_gdrive
from docs.write_dataframe import write_df_to_gdrive
import os

def main():
    # Obtém a data atual para nomear os arquivos
    today = datetime.today().strftime('%Y_%m_%d')

    # IDs das pastas no Google Drive
    folder_raw = '1k6Kz-Q2Uy4iAGlM_XrSpVgDE4SRDwSju'  # Pasta onde os arquivos são lidos
    folder_cleaned = '1zZkHkdPQynXeZH_T_nnVhHfTqktuZB-X'  # Pasta onde os arquivos são salvos

    # Nome do arquivo a ser lido
    file_name = f"{today}_products.csv"

    # Lê o arquivo da pasta raw usando a função read_gdrive
    arquivo = read_gdrive(folder_raw, file_name)  # Passa o nome do arquivo
    print(arquivo.columns)

    # Verifica se a leitura do arquivo retornou dados
    if arquivo is not None and not arquivo.empty:
        # Realiza o processamento dos dados
        arquivo.replace(r'["“”]', '', regex=True, inplace=True)
        split_columns = arquivo.iloc[:, 0].str.split(r"\(Código:", expand=True)  # Acessa a primeira coluna
        descrição = split_columns[0].str.replace(r'["“”"]', '', regex=True).replace(r',', ' ', regex=True).str.strip().str.upper()
        código = split_columns[1].str.strip(')').astype(int)
        quantidade = arquivo.iloc[:, 1].str.strip().replace(r'[^\d.]', '', regex=True).astype(float)  # Acessa a segunda coluna
        unidade = arquivo.iloc[:, 2].replace(r'.*:', '', regex=True).str.strip().str.upper()  # Acessa a terceira coluna
        valor_unitário = arquivo.iloc[:, 3].str.strip().replace(r'[^\d,]', '', regex=True).str.replace(',', '.').str.strip().astype(float)  # Acessa a quarta coluna

        # Cria um dicionário para o DataFrame
        data_dict = {
            'Descricao': descrição,
            'Codigo': código,
            'Quantidade': quantidade,
            'Unidade': unidade,
            'Valor Unitario': valor_unitário
        }

        df = pd.DataFrame(data_dict)

        # Salva o DataFrame na pasta cleaned usando a função write_df_to_gdrive
        write_df_to_gdrive(df, f"{today}_products.csv", folder_cleaned)  # Usa a nova função
    else:
        print("Nenhum dado encontrado na pasta especificada.")

if __name__ == "__main__":
    main()
