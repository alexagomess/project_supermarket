import pandas as pd
import hashlib
from typing import List


def hash_unicode(a_string: str, encoding: str = "utf-8") -> str:
    return hashlib.sha256(a_string.encode(encoding)).hexdigest()


def create_hash_column_pandas(
    df: pd.DataFrame, column_name: str, columns: List[str]
) -> pd.DataFrame:
    """
    Creates a new column in the pandas DataFrame `df` by computing the SHA-256 hash of the concatenation of the values in the
    specified columns. The hash value is stored in the new column with the name `column_name`.

    Args:
        df (pd.DataFrame): The DataFrame to which the new column will be added.
        column_name (str): The name of the new column to be added to the DataFrame.
        columns (List[str]): The list of column names to concatenate and hash.
    Returns:
        a new DataFrame with the same schema as `df`, but with the addition of the new column containing the
        SHA-256 hash values.
    """
    df[column_name] = (
        df[columns].fillna("NA").astype(str).apply(",".join, axis=1).apply(hash_unicode)
    )
    return df
