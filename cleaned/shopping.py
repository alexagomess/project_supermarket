from turtle import up
import pandas as pd
from datetime import datetime
from docs.gdrive_read import read_gdrive
from docs.write_dataframe import write_df_to_gdrive
from config import FOLDER_RAW, FOLDER_CLEANED

def main():
    folder_raw = FOLDER_RAW  # Pasta onde os arquivos são lidos
    folder_cleaned = FOLDER_CLEANED  # Pasta onde os arquivos são salvos

    # Lê todos os arquivos da pasta raw
    raw_files = read_gdrive(folder_raw)

    shopping_files = [file for file in raw_files if file.endswith('-shopping.csv')]

    for shopping_file in shopping_files:
        # Lê o arquivo da pasta raw
        arquivo = read_gdrive(folder_raw, shopping_file)

        # Inspeciona o DataFrame
        if arquivo is not None and not arquivo.empty:
            emission_date = None
            for index, row in arquivo.iterrows():
                if 'Data Emissão' in row.iloc[0]:  
                    emission_date = row.iloc[1]
                    break

            if emission_date is not None:
                # Converte a data de referência para datetime
                reference_date = datetime.strptime(emission_date, "%d/%m/%Y %H:%M:%S")
                # reference_date = emission_date.strftime('%Y-%m-%d')
                # reference_hour = emission_date.strftime('%H:%M:%S')

                # Realiza o processamento dos dados
                arquivo.replace(r'["“”]', '', regex=True, inplace=True)

                # Divide a terceira coluna em 'Descrição' e 'Código'
                split_columns = arquivo.iloc[:, 2].str.split(r"\(Código:", expand=True)

                # Verifica se a divisão gerou pelo menos 2 colunas
                if split_columns.shape[1] < 2:
                    print(f"Erro: A divisão da coluna não gerou duas partes para o arquivo {shopping_file}.")
                    continue
                
                descricao = split_columns[0].str.replace(r'["“”"]', '', regex=True).replace(r',', ' ', regex=True).str.strip().str.upper()
                código = split_columns[1].str.strip(')').astype(str)  # Mantém como string

                # Acessa as outras colunas
                quantidade = arquivo.iloc[:, 3].str.strip().replace(r'[^\d.]', '', regex=True).astype(float)
                unidade = arquivo.iloc[:, 4].replace(r'.*:', '', regex=True).str.strip().str.upper()
                valor_unitario = arquivo.iloc[:, 5].str.strip().replace(r'[^\d,]', '', regex=True).str.replace(',', '.').str.strip().astype(float)

                created_at = datetime.now()
                updated_at = datetime.now()

                # Cria um dicionário para o DataFrame
                data_dict = {
                    'descricao': descricao,
                    'codigo': código,
                    'quantidade': quantidade,
                    'unidade': unidade,
                    'valor unitario': valor_unitario,
                    'reference_date': reference_date,
                    'created_at': created_at,
                    'updated_at': updated_at
                }

                df = pd.DataFrame(data_dict)

                # Salva o DataFrame na pasta cleaned com o mesmo nome do arquivo raw
                write_df_to_gdrive(df, shopping_file, folder_cleaned)
            else:
                print(f"O arquivo {shopping_file} não contém a data de emissão.")
                continue
        else:
            print(f"O arquivo {shopping_file} está vazio ou não foi encontrado.")
            continue

if __name__ == "__main__":
    main()
