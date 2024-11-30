import pandas as pd
<<<<<<< HEAD:scripts/cleaned/nfe_information.py
import unidecode
from scripts.common.etl import load_google_drive, read_google_drive
from config import FOLDER_RAW, FOLDER_CLEANED_NFE_INFORMATION
from scripts.common.logging import Logger


def clean_currency(value):
    if isinstance(value, str):
        value = value.replace("R$", "").replace(".", "").replace(",", ".").strip()
        return float(value)
    return value


def main():
    folder_raw = FOLDER_RAW
    folder_cleaned = FOLDER_CLEANED_NFE_INFORMATION

    raw_files = read_google_drive(folder_raw)
    shopping_files = [file for file in raw_files if file.endswith("-shopping.csv")]

    for shopping_file in shopping_files:
        Logger.info(f"Lendo o arquivo: {shopping_file}")
        arquivo = read_google_drive(folder_raw, shopping_file)

        if arquivo is not None and not arquivo.empty:
            data_dict = {}
            for index, row in arquivo.iterrows():
                if pd.notnull(row.iloc[1]):
                    data_dict[row.iloc[0]] = row.iloc[1]

            df_info = pd.DataFrame(
                list(data_dict.items()), columns=["Titulo", "Informacao"]
            )

            df_pivoted = df_info.set_index("Titulo").T

            df_pivoted.columns = [
                unidecode.unidecode(str(col)).lower() for col in df_pivoted.columns
            ]

            for col in ["valor total do servico", "base de calculo icms", "valor icms"]:
                if col in df_pivoted.columns:
                    Logger.info(f"Processando a coluna: {col}")
                    df_pivoted[col] = df_pivoted[col].apply(clean_currency)
                else:
                    Logger.error(
                        f"Erro: A coluna '{col}' não foi encontrada no arquivo {shopping_file}."
                    )
                    continue

            df_pivoted.dropna(axis=1, how="all", inplace=True)
            df_pivoted.dropna(axis=0, how="all", inplace=True)

            new_file_name = shopping_file.replace("-shopping.csv", "-nfe_info.csv")
            df_pivoted = df_pivoted.dropna(
                subset=[
                    "nome / razao social",
                    "cnpj",
                    "inscricao estadual",
                    "uf",
                    "destino da operacao",
                    "consumidor final",
                    "presenca do comprador",
                    "modelo",
                    "serie",
                    "numero",
                    "data emissao",
                    "valor total do servico",
                    "base de calculo icms",
                    "valor icms",
                    "protocolo",
                    "chave de acesso",
                ],
                how="any",
            )
            if df_pivoted.empty:
                Logger.info(
                    "DataFrame está vazio após remoção de linhas nulas. Nenhum arquivo será enviado."
                )
                return

            Logger.info(f"Salvando o arquivo processado: {new_file_name}")
            load_google_drive(df_pivoted, new_file_name, folder_cleaned)
        else:
            Logger.error(
                f"Erro: O arquivo {shopping_file} está vazio ou não foi encontrado."
            )
            continue


=======
import unidecode  # Importa a biblioteca para remover acentos e cedilhas
from turtle import up
from datetime import datetime
from docs.gdrive_read import read_gdrive
from docs.write_dataframe import write_df_to_gdrive
from config import FOLDER_RAW, FOLDER_CLEANED

def clean_currency(value):
    if isinstance(value, str):  # Verifica se o valor é uma string
        value = value.replace('R$', '').replace('.', '').replace(',', '.').strip()  # Remove R$, milhar e troca vírgula por ponto
        return float(value)  # Converte para float
    return value

def main():
    folder_raw = FOLDER_RAW  # Pasta onde os arquivos são lidos
    folder_cleaned = FOLDER_CLEANED  # Pasta onde os arquivos são salvos

    # Lê todos os arquivos da pasta raw
    raw_files = read_gdrive(folder_raw)
    shopping_files = [file for file in raw_files if file.endswith('-shopping.csv')]

    for shopping_file in shopping_files:
        # Lê o arquivo da pasta raw
        print(f"Lendo o arquivo: {shopping_file}")
        arquivo = read_gdrive(folder_raw, shopping_file)

        # Inspeciona o DataFrame
        if arquivo is not None and not arquivo.empty:
            # Cria um dicionário para armazenar os dados da primeira e segunda coluna
            data_dict = {}
            for index, row in arquivo.iterrows():
                # Adiciona os dados ao dicionário, garantindo que não sejam vazios
                if pd.notnull(row.iloc[1]):  # Verifica se a informação não é NaN
                    data_dict[row.iloc[0]] = row.iloc[1]

            # Cria um DataFrame a partir do dicionário
            df_info = pd.DataFrame(list(data_dict.items()), columns=['Titulo', 'Informacao'])

            # Pivotar a tabela
            df_pivoted = df_info.set_index('Titulo').T

            # Renomear colunas: minúsculas e sem acentos
            df_pivoted.columns = [unidecode.unidecode(str(col)).lower() for col in df_pivoted.columns]

            # Verifica se as colunas necessárias estão presentes e processa
            for col in ["valor total do servico", "base de calculo icms", "valor icms"]:
                if col in df_pivoted.columns:
                    print(f"Processando a coluna: {col}")
                    df_pivoted[col] = df_pivoted[col].apply(clean_currency)
                else:
                    print(f"Erro: A coluna '{col}' não foi encontrada no arquivo {shopping_file}.")
                    continue

            # Remove colunas com todos os valores como NaN
            df_pivoted.dropna(axis=1, how='all', inplace=True)

            # Remove linhas com todos os valores NaN
            df_pivoted.dropna(axis=0, how='all', inplace=True)

            # Cria o novo nome do arquivo
            new_file_name = shopping_file.replace('-shopping.csv', '-nfe_info.csv')

            # Salva o DataFrame na pasta cleaned com o novo nome do arquivo
            print(f"Salvando o arquivo processado: {new_file_name}")
            write_df_to_gdrive(df_pivoted, new_file_name, folder_cleaned)
        else:
            print(f"Erro: O arquivo {shopping_file} está vazio ou não foi encontrado.")
            continue

>>>>>>> 0675c5609241d6abb652d74402008b13f640264f:cleaned/nfe_information.py
if __name__ == "__main__":
    main()
