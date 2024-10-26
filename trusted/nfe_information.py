import pandas as pd
from docs.gdrive_read import read_gdrive
from docs.write_dataframe import write_df_to_gdrive
from config import FOLDER_CLEANED, FOLDER_TRUSTED_NFE_INFORMATION


def save_nfe_info():
    folder_cleaned = FOLDER_CLEANED  # Pasta onde os arquivos são salvos
    folder_trusted = FOLDER_TRUSTED_NFE_INFORMATION  # Pasta onde os arquivos da camada trusted serão salvos

    # Lê o arquivo de nfe_info da pasta cleaned
    cleaned_files = read_gdrive(folder_cleaned)
    for cleaned_file in cleaned_files:
        if cleaned_file.endswith("-nfe_info.csv"):
            df_nfe_info = read_gdrive(folder_cleaned, cleaned_file)

            # Obtém a data de emissão para o nome do arquivo
            if not df_nfe_info.empty:
                emission_date = df_nfe_info["data emissao"].iloc[0]
                formatted_date = pd.to_datetime(emission_date).strftime(
                    "%Y-%m-%d-%H-%M-%S"
                )
                file_name = f"{formatted_date}_nfe_info.csv"

                write_df_to_gdrive(df_nfe_info, file_name, folder_trusted)
                print(f"Arquivo '{file_name}' salvo com sucesso.")


if __name__ == "__main__":
    save_nfe_info()
