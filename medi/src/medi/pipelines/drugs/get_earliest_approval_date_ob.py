import pandas as pd 
from tqdm import tqdm 
from datetime import datetime

def get_approval_dates(df: pd.DataFrame, ingredient: str) -> list[str]:
    """
    Return all approval dates for a given ingredient string in the orange book dataframe.

    Parameters:
        ob (pd.DataFrame): orange book dataframe
        ingredient (str): the ingredient to extract dates from 

    Returns:
        list[str]: list of dates. Note: may include the phrase 'Approved Prior to Jan 1, 1982' 
    """
    mask = df['source_ingredients']==ingredient
    ing_df = df[mask]
    return list(ing_df['approval_date'])
    #indices = [i for i,x in enumerate(list(ob['Ingredient'])) if x==ingredient]
    
    #dates = list(ob["Approval_Date"][indices])
    # print(type(dates[0]))
    # print(ingredient)
    # print(dates)
    #return dates


def get_earliest_date_list(date_list):
    """
    Return the earliest date from a list of 'dd-mon-yy' format strings,
    formatted as 'yyyymmdd'.

    Parameters:
        date_list (list of str): List of dates in 'dd-mon-yy' format

    Returns:
        str: Earliest date in 'yyyymmdd' format
    """
    if not date_list:
        raise ValueError("The date list is empty.")

    try:
        # Parse all dates
        parsed_dates = [datetime.strptime(date, '%b %d, %Y') for date in date_list]
        # Find earliest
        earliest_date = min(parsed_dates)
        return earliest_date.strftime('%Y%m%d')
    except ValueError as e:
        raise ValueError(f"One or more dates are invalid: {e}")


def get_earliest_date_item(ob: pd.DataFrame, item: str) -> str:
    approval_dates = get_approval_dates(ob, item)
    if "Approved Prior to Jan 1, 1982" in approval_dates:
        return "19820101"
    else:
        return get_earliest_date_list(approval_dates)


def acquire_earliest_approval_dates(ob: pd.DataFrame) -> pd.DataFrame:
    """
    Return dataframe with approval dates replaced with the earliest approval date.

    Parameters:
        ob (pd.DataFrame) orange book
    
    Returns:
        pd.DataFrame: data frame with dates replaced with earliest dat for each 

    """
    cache = {}
    new_dates = []
    for idx, row in tqdm(ob.iterrows()):
        ingredient = row['source_ingredients']
        if ingredient in cache:
            new_dates.append(cache[ingredient])
        else:
            date = get_earliest_date_item(ob, ingredient)
            cache[ingredient]=date
            new_dates.append(date)
    ob['approval_date'] = new_dates
    return ob


