import pandas as pd 
from tqdm import tqdm 

def getAllStatuses(ob: pd.DataFrame, item: str) -> list[str]:
    """
    Args:
        orangebook (pd.DataFrame): orange book raw data
        item (str): name of the drug whose statuses are to be returned

    Returns:
        list[str]: availability status of all drug formulations for named drug in the United States

    """

    indices = [i for i, x in enumerate(ob['source_ingredients']) if x == item]
    return list(ob['marketing_status_usa'][indices])

def getMostPermissiveStatus(statusList: list[str]) -> str:
    """
    Args:
        statusList (list[str]): list of statuses for a particular chemical entity

    Returns:
        str: The most permissive availability status for the chemical entity in the US (over the counter > RX > DISCN) or "UNSURE" if not clear.

    """
    if "OTC" in statusList:
        return "OTC"
    elif "RX" in statusList or "Rx" in statusList:
        return "RX"
    elif "DISCN" in statusList or "Disc" in statusList or "Disc*" in statusList:
        return "DISCONTINUED"
    return "UNSURE"

def add_most_permissive_marketing_tags_fda(in_list: pd.DataFrame) -> pd.DataFrame:
    cache = {}
    for _, row in tqdm(in_list.iterrows(), total=len(in_list), desc = "caching drug marketing statuses"):
        if row['source_ingredients'] not in cache:
            cache[row['source_ingredients']]=getMostPermissiveStatus(getAllStatuses(in_list, row['source_ingredients']))
    new_approval_tags_column = []
    for _, row in tqdm(in_list.iterrows(), total = len(in_list), desc = "adding marketing status labels"):
        new_approval_tags_column.append(cache[row['source_ingredients']])
    in_list['marketing_status_usa']=new_approval_tags_column
    #in_list.rename(columns={"Type":"marketing_status_usa"}, inplace=True)
    return in_list