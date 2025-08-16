from tqdm import tqdm 
import pandas as pd
import requests
import json
from functools import cache


def normalize_multiple_columns(df:pd.DataFrame, column_names:list[str]) -> pd.DataFrame:
    for item in column_names:
        df = normalize_column(df, item)
    return df

def normalize_column(df:pd.DataFrame, column_name:str) -> pd.DataFrame:
    '''
    Args:
        df: dataframe with column of properly-formatted IDs (ONTOLOGY:ID_NO)
        column_name: name of column to be normalized
    
    Returns:
        pd.DataFrame: dataframe with new columns column_name_norm and column_name_norm_label

    '''
    cache = {}
    norm_id = []
    norm_label = []
    alt_ids_col = []
    id = ""
    label = ""
    for _, row in tqdm(df.iterrows(), total=len(df), desc="normalizing"):
        name = row[column_name]
        if name in cache:
            id, label, alt_ids = cache[name]
            norm_id.append(id)
            norm_label.append(label)
            alt_ids_col.append(alt_ids)
        else:  
            id, label, alt_ids = normalize(name)
            cache[name]=id, label, alt_ids
            norm_id.append(id)
            norm_label.append(label)
            alt_ids_col.append(alt_ids)
    
    df[f"{column_name}_norm"]=norm_id
    df[f"{column_name}_norm_label"]=norm_label
    df["alternate_ids"]=alt_ids_col
    return df


@cache
def normalize(item: str):
    item_request = f"https://nodenormalization-sri.renci.org/1.5/get_normalized_nodes?curie={item}&conflate=true&drug_chemical_conflate=true&description=false&individual_types=false"    
    success = False
    failedCounts = 0
    while not success:
        try:
            response = requests.get(item_request)
            output = json.loads(response.text)
            primary_key = list(output.keys())[0]
            label = output[primary_key]['id']['label']
            id = output[primary_key]['id']['identifier']
            alternate_ids = output[item]['equivalent_identifiers']
            #returned_ids = list(item['identifier'] for item in alternate_ids)
            success = True
        except Exception as e:
            print(f"exception on {item}")
            print(e) 
            #print('name resolver error')
            failedCounts += 1
        if failedCounts >= 5:
            return ["Error"], ["Error"], ['Error']
    return id, label, alternate_ids