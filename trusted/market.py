import pandas as pd
import os
from datetime import datetime
from docs.gdrive_read import read_gdrive
from docs.write_dataframe import write_df_to_gdrive
from dotenv import load_dotenv
from config import FOLDER_CLEANED, FOLDER_TRUSTED_MARKET


def save_market():
    load_dotenv()
    folder_cleaned = FOLDER_CLEANED # Pasta onde os arquivos são salvos
    folder_trusted = FOLDER_TRUSTED_MARKET  # Pasta onde os arquivos da camada trusted serão salvos

    # Lê o arquivo de nfe_info da pasta cleaned
    cleaned_files = read_gdrive(folder_cleaned)
    for cleaned_file in cleaned_files:
        if cleaned_file.endswith('-nfe_info.csv'):
            df_nfe_info = read_gdrive(folder_cleaned, cleaned_file)

            # Cria tabela de mercado
            if not df_nfe_info.empty:
                df_mercado = df_nfe_info[['nome / razao social', 'cnpj', 'inscricao estadual', 'uf']].copy()
                df_mercado.columns = ['nome mercado', 'cnpj', 'inscricao estadual', 'uf']  # Renomeia as colunas
                df_mercado['created_at'] = datetime.now()
                df_mercado['updated_at'] = datetime.now()

                # Obtém a data de emissão para o nome do arquivo
                emission_date = df_nfe_info['data emissao'].iloc[0]
                formatted_date = pd.to_datetime(emission_date).strftime('%Y-%m-%d-%H-%M-%S')
                file_name = f'{formatted_date}_market.csv'

                # Salva o arquivo de mercado na camada trusted
                if not df_mercado.empty:
                    write_df_to_gdrive(df_mercado, file_name, folder_trusted)
                    print(f"Arquivo '{file_name}' salvo com sucesso.")

if __name__ == "__main__":
    save_market()
