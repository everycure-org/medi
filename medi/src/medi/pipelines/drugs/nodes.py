"""
This is a boilerplate pipeline 'drugs'
generated using Kedro 0.19.14
"""
import pandas as pd 
import zipfile
from pathlib import Path

import tempfile
from tqdm import tqdm


def standardize_dataframe (df, col_mapping):
    """
    Extracts specified columns from a DataFrame and renames them.

    Args:
        df (pd.DataFrame): The input DataFrame.
        col_mapping (dict): Dictionary where keys are source column names,
                            and values are new column names.

    Returns:
        pd.DataFrame: A new DataFrame with selected and renamed columns.
    """
    return df[list(col_mapping.keys())].rename(columns=col_mapping)

def add_full_column_identical_strings(df, colname:str, value: str) -> pd.DataFrame:
    df[colname]=value
    return df

def deduplicate_dataframe(df: pd.DataFrame, dedup_cols: list) -> pd.DataFrame:
    """
    Deduplicates a DataFrame based on a subset of columns.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        dedup_cols (list): List of column names to deduplicate on.

    Returns:
        pd.DataFrame: A deduplicated DataFrame.
    """
    if not all(col in df.columns for col in dedup_cols):
        missing = [col for col in dedup_cols if col not in df.columns]
        raise ValueError(f"Columns not found in DataFrame: {missing}")

    deduped_df = df.drop_duplicates(subset=dedup_cols).reset_index(drop=True)
    return deduped_df
