import pandas as pd
from scripts.docs.gdrive_read import read_gdrive
from scripts.docs.write_dataframe import write_df_to_gdrive
from config import FOLDER_CLEANED, FOLDER_TRUSTED_SHOPPING


def save_shopping():
    folder_cleaned = FOLDER_CLEANED  # Pasta onde os arquivos são salvos
    folder_trusted = (
        FOLDER_TRUSTED_SHOPPING  # Pasta onde os arquivos da camada trusted serão salvos
    )

    # Lê o arquivo de shopping da pasta cleaned
    cleaned_files = read_gdrive(folder_cleaned)
    for cleaned_file in cleaned_files:
        if cleaned_file.endswith("-shopping.csv"):
            df_shopping = read_gdrive(folder_cleaned, cleaned_file)

            # Obtém a data de emissão para o nome do arquivo
            if not df_shopping.empty:
                emission_date = df_shopping["reference_date"].iloc[0]
                formatted_date = pd.to_datetime(emission_date).strftime(
                    "%Y-%m-%d-%H-%M-%S"
                )
                file_name = f"{formatted_date}_shopping.csv"

                write_df_to_gdrive(df_shopping, file_name, folder_trusted)
                print(f"Arquivo '{file_name}' salvo com sucesso.")


if __name__ == "__main__":
    save_shopping()
