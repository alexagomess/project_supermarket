import pandas as pd
from scripts.common.logging import Logger


def preprocess_dates(df, date_columns):
    """
    Converte colunas de data para o formato ISO-8601 (YYYY-MM-DD HH:MM:SS).
    """
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column], format="%d/%m/%Y %H:%M:%S", errors="coerce"
            )
            if df[column].isnull().any():
                Logger.warning(
                    f"Algumas datas na coluna '{column}' não puderam ser convertidas e foram substituídas por NaT."
                )
    return df
