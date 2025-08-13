import pandas as pd
from io import StringIO
import requests
from tqdm import tqdm
from functools import cache



def identify(name: str, params: dict):
    """
    Args:
        name (str): string to be identified
        params (tuple): name resolver parameters to feed into get request
    
    Returns:
        resolvedName (str): IDs most closely matching string.
        resolvedLabel (str): labels associated with respective resolvedName items.

    """

    if not name or type(name) == float:
        print("No name provided or blank name provided")
        return ['Error'], ['Error']
    # need space following semicolon delimiter but eliminate double spaces
    name = name.replace(";", "; ").replace("  ", " ")
    itemRequest = (params['url']+
                   params['service']+
                   '?string='+
                   name+
                   '&autocomplete='+
                   str(params['autocomplete_setting']).lower()+
                   '&offset='+
                   str(params['offset'])+
                   '&limit='+
                   str(params['id_limit']))
    success = False
    failedCounts = 0
    
    while not success:
        try:
            returned = (pd.read_json(StringIO(requests.get(itemRequest).text)))
            resolvedName = returned.curie
            resolvedLabel = returned.label
            success = True
        except:
            #print('name resolver error')
            failedCounts += 1
        if failedCounts >= 5:
            print(f"could not resolve concept {name}")
            return ["Error"], ["Error"]
   
    return resolvedName[0], resolvedLabel[0]


def nameres_column (df: pd.DataFrame, colname: str, params: dict) -> pd.DataFrame:
    out_curie = []
    out_label = []
    cache = {}
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="resolving column..."):
        if row[colname] in cache:
            curie, label = cache[row[colname]]
        else:
            curie, label = identify(row[colname], params)
            cache[row[colname]]=curie,label
        out_curie.append(curie)
        out_label.append(label)
    
    df[f"{colname}_curie"]=out_curie
    df[f"{colname}_curie_label"]=out_label

    return df

def nameres_multiple_columns(df: pd.DataFrame, colnames: list[str], params:dict) -> pd.DataFrame:
    for item in colnames:
        df = nameres_column(df, item, params)
    
    return df


def nameres_column_combination_therapy_ingredients(df: pd.DataFrame, colname: str, params:dict) -> pd.DataFrame:
    cache = {}
    out_curies = []
    for _, row in tqdm(df.iterrows(), total = len(df), desc = "resolving combination therapy components"):
        inglist = row[colname]
        if inglist == "" or type(inglist)==float:
            out_curies.append("")
        else:
            curielist = []
            for item in inglist.split("|"):
                if item in cache:
                    curielist.append(cache[item])
                else:
                    curie = identify(item, params)
                    cache[item]=curie
                    curielist.append(cache[item])
            out_curies.append(curielist)
    df[f"{colname}_curies"]=out_curies
    return df