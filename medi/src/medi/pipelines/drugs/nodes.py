"""
This is a boilerplate pipeline 'drugs'
generated using Kedro 0.19.14
"""
import pandas as pd 
import zipfile
from pathlib import Path

import tempfile
from tqdm import tqdm
from medi.utils import openai_tags, nameres
import numpy as np
from openai import OpenAI

def combine_rows(series):
        unique_values = series.dropna().unique()
        if len(unique_values) == 0:
            return np.nan
        elif len(unique_values) == 1:
            return unique_values[0]
        else:
            # If multiple non-null values exist, join them with semicolons
            return '| '.join(str(x) for x in unique_values)

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

    deduped_df = df.groupby(dedup_cols, as_index=False).agg(combine_rows)
    # deduped_df = df.drop_duplicates(subset=dedup_cols).reset_index(drop=True)
    return deduped_df

def deduplicate_with_join(df, dedupe_columns, join_delimiter='|'):
    """
    Deduplicate a DataFrame on specified columns and join other columns with a delimiter.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    dedupe_columns (list): List of column names to deduplicate on
    join_delimiter (str): Delimiter to join duplicate values (default: '|')
    
    Returns:
    pd.DataFrame: Deduplicated DataFrame with other columns joined
    """
    # Get columns that are not in the dedupe list
    other_columns = [col for col in df.columns if col not in dedupe_columns]
    
    # Group by the dedupe columns
    grouped = df.groupby(dedupe_columns, as_index=False)
    
    # For each group, join the other columns with the delimiter
    result_data = []
    for name, group in grouped:
        row_dict = {}
        
        # Add the dedupe columns (these will be the same for all rows in the group)
        for col in dedupe_columns:
            row_dict[col] = group[col].iloc[0]
        
        # Join the other columns with the delimiter
        for col in other_columns:
            # Convert to string and remove duplicates while preserving order
            values = group[col].astype(str).tolist()
            unique_values = []
            seen = set()
            for val in values:
                if val not in seen:
                    unique_values.append(val)
                    seen.add(val)
            row_dict[col] = join_delimiter.join(unique_values)
        
        result_data.append(row_dict)
    
    return pd.DataFrame(result_data)

def create_single_unlisted_ingredient(row, curie, label, source)->pd.DataFrame:
    """
    Takes a single row of a dataframe with an unlisted ingredient and generates a df
    with a single row for that ingredient.

    Parameters:
        row: the row of the dataframe containing the combination therapy with the unlisted ingredient
        curie (str): the curie of the ingredient
        label (str): the label of the ingredient
    Returns:
        pd.DataFrame: 
    
    """
    row['source_ingredients']=source
    row['source_ingredients_curie']=curie
    row['source_ingredients_curie_label']=label
    row['is_combination_therapy']="FALSE"
    row['combination_therapy_ingredients']=""
    row['combination_therapy_ingredients_curies']=""
    return pd.DataFrame(row)

def add_unlisted_ingredients(df:pd.DataFrame)-> pd.DataFrame:
    """
    Parses through drug list dataframe and adds ingredients in coformulated drugs that are not listed as
    top-class items in the list as top-class entities.

    Parameters:
        df (pd.DataFrame): dataframe with current items and no unlisted ingredients added.

    Returns:
        pd.DataFrame: dataframe with unlisted drug components added onto the end of the list

    """
    druglist = list(df['source_ingredients_curie'])
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="adding unlisted combo therapy ingredients to list..."):
        druglist = list(df['source_ingredients_curie'])
        ings = row['combination_therapy_ingredients_curies']
        if type(ings)!= float and ings!="":
            ing_list = ings.split("|")
            for ing in ing_list:
                curie_label = ing.split("~")
                if curie_label[0] not in druglist:
                    row = pd.DataFrame([df.iloc[idx]])
                    df = pd.concat([df,create_single_unlisted_ingredient(row, curie_label[0], curie_label[1], ing)], axis=0)    
    return df


def split_combination_therapies(df:pd.DataFrame, params: dict)->pd.DataFrame:
    split_ingredients = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="splitting combination therapies"):
        if row['is_combination_therapy']==True:
            prompt = f"{params['model_params']['prompt']}{row['source_ingredients']}"
            split_ingredients.append(openai_tags.single_openai_prompt(prompt = prompt, model=params['model_params']['model'], temperature=params['model_params']['temperature']  ))
        else:
            split_ingredients.append("")
    
    df[params['output_col']]=split_ingredients
    return df

def qc_id_llm(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    prompts_col = [f"Drug 1: {row['source_ingredients']}; Drug 2: {row['source_ingredients_curie_label']}" for idx, row in df.iterrows()]
    df['llm_qc_comparison_col'] = prompts_col
    df = openai_tags.add_tags(df, params, 'llm_qc_comparison_col')
    return df

def build_improve_ids_prompt(concept: str, ids: list[str], labels: list[str]):
    ids_and_names = []
    for idx, item in enumerate(ids):
        ids_and_names.append(f"{idx+1}: {item} ({labels[idx]})")   
    ids_and_names = ";\n".join(ids_and_names)
    return f"Drug Concept: {concept}. \r\n\n Options: {ids_and_names}"

def improve_ids(df: pd.DataFrame, nameres_params:dict, base_prompt: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    client = OpenAI()
    corrected_id_column = []
    for _, row in tqdm(df.iterrows(), total = len(df), desc = "improving IDs"):
        if row['id_correct']==True:
            corrected_id_column.append(row['source_ingredients_curie'])
        else:
            ids, labels = nameres.nameres(row['source_ingredients'], nameres_params)
            prompt = f"{base_prompt} {build_improve_ids_prompt(row['source_ingredients'], list(ids), list(labels))}"
            try:
                response = client.responses.create(
                    model="gpt-4o-mini",
                    input=prompt
                )
                corrected_id_column.append(response.output_text)
            except Exception as e:
                corrected_id_column.append("Error")
    df['corrected_curie']=corrected_id_column
    
    not_resolved_errors = df[df['corrected_curie']=="Error" ]
    not_found_errors = df[df['corrected_curie']=="NONE"]

    errors = pd.concat([not_resolved_errors, not_found_errors])


    df = df[df['corrected_curie']!="Error"]
    df = df[df['corrected_curie']!="NONE"]


    print(errors)
    return df, errors

def join_lists(orangebook, purplebook, ema, pmda)->pd.DataFrame:
    """
    Merge multiple dataframes based on the 'curie' field, combining matching rows.
    
    Parameters:
    df_list (list): List of pandas dataframes to merge
    
    Returns:
    pandas.DataFrame: Merged dataframe with combined rows
    """
    df_list = list([pmda, ema, orangebook, purplebook])#, #russia, india])
    if not df_list:
        raise ValueError("Empty list of dataframes provided")
        
    if len(df_list) == 1:
        return df_list[0]
    combined_df = pd.concat(df_list, ignore_index=True)
    
    
    # Group by 'curie' and aggregate all other columns
    merged_df = combined_df.groupby('corrected_curie', as_index=False).agg(combine_rows)
    return merged_df


