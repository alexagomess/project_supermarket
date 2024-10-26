import pandas as pd
from datetime import datetime
from docs.gdrive_read import read_gdrive
from docs.write_dataframe import write_df_to_gdrive
from config import FOLDER_CLEANED, FOLDER_TRUSTED_PRODUCTS

def save_products():
    folder_cleaned = FOLDER_CLEANED  # Pasta onde os arquivos são salvos
    folder_trusted = FOLDER_TRUSTED_PRODUCTS  # Pasta onde os arquivos da camada trusted serão salvos

    # Lê o arquivo de shopping da pasta cleaned
    cleaned_files = read_gdrive(folder_cleaned)
    for cleaned_file in cleaned_files:
        if cleaned_file.endswith('-shopping.csv'):
            df_shopping = read_gdrive(folder_cleaned, cleaned_file)

            # Cria tabela de produtos
            if not df_shopping.empty:
                df_produtos = df_shopping[['descricao', 'codigo']].drop_duplicates()
                df_produtos['ean'] = None  # Adiciona coluna EAN (vazia por enquanto)
                df_produtos['created_at'] = datetime.now()
                df_produtos['updated_at'] = datetime.now()

                # Obtém a data de emissão para o nome do arquivo
                emission_date = df_shopping['reference_date'].iloc[0]
                formatted_date = pd.to_datetime(emission_date).strftime('%Y-%m-%d-%H-%M-%S')
                file_name = f'{formatted_date}_products.csv'

                # Salva o arquivo de produtos na camada trusted
                if not df_produtos.empty:
                    write_df_to_gdrive(df_produtos, file_name, folder_trusted)
                    print(f"Arquivo '{file_name}' salvo com sucesso.")

if __name__ == "__main__":
    save_products()
