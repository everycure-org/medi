"""
This is a boilerplate pipeline 'drugs'
generated using Kedro 0.19.14
"""
import pandas as pd 
import zipfile
from pathlib import Path

import tempfile
from tqdm import tqdm
from medi.utils import openai_tags, nameres, normalize
import numpy as np
from openai import OpenAI
from . import grouped_bar
from matplotlib_venn import venn2
import matplotlib.pyplot as plt

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
    row['corrected_curie_norm']=curie
    row['corrected_curie_norm_label']=label
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
    merged_df = df.groupby('corrected_curie_norm', as_index=False).agg(combine_rows)
  
    return merged_df


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

def join_lists(orangebook, purplebook, ema, pmda, russia, india)->pd.DataFrame:
    """
    Merge multiple dataframes based on the 'curie' field, combining matching rows.
    
    Parameters:
    df_list (list): List of pandas dataframes to merge
    
    Returns:
    pandas.DataFrame: Merged dataframe with combined rows
    """
    df_list = list([pmda, ema, orangebook, purplebook, russia, india])
    if not df_list:
        raise ValueError("Empty list of dataframes provided")
        
    if len(df_list) == 1:
        return df_list[0]
    combined_df = pd.concat(df_list, ignore_index=True)
    
    
    # Group by 'curie' and aggregate all other columns
    merged_df = combined_df.groupby('corrected_curie_norm', as_index=False).agg(combine_rows)
    # Remove problematic characters for Excel/openpyxl
    merged_df = merged_df.replace({
        r'[^\x20-\x7E\t\n\r]': '',  # Remove non-printable chars (keep tabs, newlines, carriage returns)
        r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]': '',  # Remove control characters except \t, \n, \r
        r'[\uFEFF]': '',  # Remove BOM (Byte Order Mark)
        r'[\u200B-\u200D\uFEFF]': '',  # Remove zero-width spaces and similar
    }, regex=True)
    return merged_df


def compare(previous_list: pd.DataFrame, current_list: pd.DataFrame) -> pd.DataFrame:
    
    #drugs_old = set(previous_list['improved_id'])
    #drugs_new = set(current_list['improved_id'])

    drugs_old = set(previous_list['corrected_curie_norm'])
    drugs_new = set(current_list['corrected_curie_norm'])

    drugs_removed = drugs_old.difference(drugs_new)
    drugs_added = drugs_new.difference(drugs_old)
    drugs_same = drugs_new.intersection(drugs_old)

    print(f"{len(drugs_removed)} drugs removed from list : {drugs_removed}")
    print(f"{len(drugs_added)} drugs added to list: {drugs_added}")
    print(f"{len(drugs_same)} drugs remain the same between versions.")

    #print(previous_list)
    #print(current_list)

    drugs_added_labels = (normalize.normalize(item)[1] for item in tqdm(drugs_added, desc="normalizing added drugs"))
    drugs_removed_labels = (normalize.normalize(item)[1] for item in tqdm(drugs_removed, desc="normalizing removed drugs"))
    drugs_same_labels = (normalize.normalize(item)[1] for item in tqdm(drugs_same, desc="normalizing unchanged drugs"))

    return pd.DataFrame({
        "drugs_added": pd.Series(list(drugs_added)),
        "added_label": pd.Series(list(drugs_added_labels)),
        "drugs_removed": pd.Series(list(drugs_removed)),
        "removed_label": pd.Series(list(drugs_removed_labels)),
        "drugs_same": pd.Series(list(drugs_same)),
        "same_label":pd.Series(list(drugs_same_labels))
    })

def store_previous_version(in_list: pd.DataFrame) -> pd.DataFrame:
    return in_list

def filter_drugs(in_df:pd.DataFrame) -> pd.DataFrame:
    indices_to_remove = []
    for idx, row in tqdm(in_df.iterrows(), total=len(in_df), desc="clearing allergens, vaccines, radioisotopes, and drugs of low therapeutic value"):
        #print(f"row['isallergen']: {row['is_allergen']}, {type(row['is_allergen'])}")
        if row['is_allergen']==True or row['is_allergen']=='TRUE' or row['is_radioisotope_or_diagnostic_agent']==True or row['is_no_therapeutic_value']==True or row['is_vaccine_or_antigen']==True:
            indices_to_remove.append(idx)
    in_df.drop(indices_to_remove, axis=0, inplace=True)
    in_df = in_df.rename(columns={'improved_id': 'curie', 'label': 'curie_label'})
    return in_df

def include_stringent_only(df:pd.DataFrame, tags: list[str]) -> pd.DataFrame:
    mask = df[tags].any(axis=1)
    new_df = df[mask]
    df_final = new_df.drop(['approved_india', 'approved_russia'], axis=1)
    return df_final

def compare_drugcentral_drugbank(druglist_stringent: pd.DataFrame, druglist_flexible: pd.DataFrame, usa: pd.DataFrame, eur: pd.DataFrame, jpn: pd.DataFrame) -> pd.DataFrame:
    cols = ['drug_id', 'drug_name']
    usa.columns = cols
    eur.columns = cols
    jpn.columns = cols
    usa['approved_usa']=[True for idx, row in usa.iterrows()]
    eur['approved_europe']=[True for idx, row in eur.iterrows()]
    jpn['approved_japan']=[True for idx, row in jpn.iterrows()]

    df_list = list([usa, eur, jpn])
    combined_df = pd.concat(df_list, ignore_index=True)
    drugcentral_merged = combined_df.groupby('drug_name', as_index=False).agg(combine_rows)
    drugcentral_merged['drug_id_ont']=[f"DRUGCENTRAL:{row['drug_id']}" for idx, row in drugcentral_merged.iterrows()]
    drugcentral_norm = drugcentral_merged
    drugcentral_norm = normalize.normalize_column(drugcentral_merged, "drug_id_ont")

    n_drugs_medi_stringent = len(druglist_stringent)
    n_drugs_medi_flexible = len(druglist_flexible)
    n_drugs_drugcentral = len(drugcentral_norm)

    n_drugs_medi_usa = len(druglist_flexible[druglist_flexible['approved_usa']==True])
    n_drugs_medi_europe = len(druglist_flexible[druglist_flexible['approved_europe']==True])
    n_drugs_medi_japan = len(druglist_flexible[druglist_flexible['approved_japan']==True])

    n_drugs_drugcentral_usa = len(usa)
    n_drugs_drugcentral_ema = len(eur)
    n_drugs_drugcentral_pmda = len(jpn)


    print(n_drugs_medi_stringent)
    print(n_drugs_medi_flexible)
    print(n_drugs_drugcentral)
    print(n_drugs_medi_usa)
    print(n_drugs_medi_europe)
    print(n_drugs_medi_japan)
    print(n_drugs_drugcentral_usa)
    print(n_drugs_drugcentral_ema)
    print(n_drugs_drugcentral_pmda)

    # EXTRACTED FROM DRUGBANK 2025-08-24
    drugbank_n_smallmolecule_usa = 2040 # Small Molecule drugs Approved or Withdrawn US market availability
    drugbank_n_biologic_usa = 364 # Biotech drugs approved or withdrawn (protein or nucleic acid) US Availability
    drugbank_n_smallmolecule_eur = 564 # Small Molecule Drugs approved or withdrawn EU availability
    drugbank_n_biologic_eur = 245 # Biotech drugs approved or withdrawn (protein or nucleic acid) EU availability
    drugbank_n_total_small_molecule = 3005 #small molecule drugs approved or withdrawn
    drugbank_n_total_biologic = 455 #small molecule drugs approved or withdrawn (protein or nucleic acid) any market availability


    # Basic usage with sample data
    #fig, ax = grouped_bar.create_grouped_bar_chart()

    # With custom data
    my_data = pd.DataFrame({
        'MeDI': [n_drugs_medi_usa, n_drugs_medi_europe, n_drugs_medi_japan, n_drugs_medi_flexible],
        'Drug Central': [n_drugs_drugcentral_usa, n_drugs_drugcentral_ema, n_drugs_drugcentral_pmda, n_drugs_drugcentral], 
        'DrugBank': [(drugbank_n_smallmolecule_usa+drugbank_n_biologic_usa), (drugbank_n_smallmolecule_eur+drugbank_n_biologic_eur),0, (drugbank_n_total_small_molecule+drugbank_n_total_biologic)],

    }, index=['USA', 'Europe', 'Japan', 'Overall Approved'])

    # Create chart with custom parameters
    fig, ax = grouped_bar.create_grouped_bar_chart(
        data=my_data,
        title="Comparison of Drug Count",
        xlabel="Region of Approval",
        ylabel="Number of Drugs",
        save_path="drug_count_comparison_medi.png"
    )
    
    drugcentral_drugs = set(list(drugcentral_norm['drug_id_ont_norm']))
    medi_drugs = set(list(druglist_flexible['curie']))
    out = venn2([drugcentral_drugs, medi_drugs],("Drug Central", "MeDI"))
    
    for text in out.set_labels:
        text.set_fontsize(22)
    for text in out.subset_labels:
        text.set_fontsize(22)
    plt.show()

    return drugcentral_norm
