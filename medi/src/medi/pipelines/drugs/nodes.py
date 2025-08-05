"""
This is a boilerplate pipeline 'drugs'
generated using Kedro 0.19.14
"""
import pandas as pd 
import zipfile
from pathlib import Path

import tempfile
from tqdm import tqdm


def standardize_dataframe(df, cols_in, cols_out):
    """
    Select and rename columns in a DataFrame according to mapping dicts.

    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    - cols_in (dict): Mapping of desired column names to actual column names in df.
    - cols_out (dict): Mapping of actual column names to new column names for output.

    Returns:
    - pd.DataFrame: A new DataFrame with selected and renamed columns.
    """
    # Map input keys to actual column names
    selected_cols = {key: cols_in[key] for key in cols_in if cols_in[key] in df.columns}

    # Create inverse map of cols_out: actual_col_name -> new_col_name
    rename_map = {cols_in[k]: cols_out[v] if v in cols_out else v for k, v in cols_in.items() if cols_in[k] in df.columns}

    # Select and rename
    return df[list(selected_cols.values())].rename(columns=rename_map)

def add_full_column_identical_strings(df, colname:str, value: str) -> pd.DataFrame:
    df[colname]=value
    return df

def deduplicate(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    




