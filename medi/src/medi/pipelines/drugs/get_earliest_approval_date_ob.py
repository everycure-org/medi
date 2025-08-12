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
    #rint(ing_df)
    return list(ing_df['approval_date'])


# def get_earliest_date_pb(df: pd.DataFrame, ingredient: str)-> str:
#     cache = {}
#     for idx, row in df.iterrows():
#         ingredient = row['source_ingredients']
#         approval_dates = get_approval_dates(df, ingredient)
#         print(approval_dates)
#         if approval_dates == None:
#             return "00000000"
#         return min(approval_dates)

def transform_dates_to_earliest(df:pd.DataFrame) -> pd.DataFrame:
    cache = {}
    approval_dates = []
    for _, row in tqdm(df.iterrows(), total = len (df), desc = "transforming dates to earliest"):
        ingredient = row['source_ingredients']
        print(ingredient)
        if ingredient in cache:
            print(cache[ingredient])
            approval_dates.append(cache[ingredient])
        else:
            earliest_date = min(get_approval_dates(df, ingredient))
            print(earliest_date)
            approval_dates.append(earliest_date)
            cache[ingredient]=earliest_date
        
    df['approval_date']=approval_dates
    return df

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
        parsed_dates = [date for date in date_list]
        print(min(parsed_dates))
        return min(parsed_dates).strftime('%Y%m%d')
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
        print(type(row['approval_date']))
        print(row('approval_date'))
        ingredient = row['source_ingredients']
        if ingredient in cache:
            new_dates.append(cache[ingredient])
        else:
            date = get_earliest_date_item(ob, ingredient)
            cache[ingredient]=date
            new_dates.append(date)
    ob['approval_date'] = new_dates
    return ob


def convert_date_format(df):
    """
    Convert approval_date column from "March 14, 2025" format to YYYYMMDD format.
    
    Parameters:
    df (pd.DataFrame): DataFrame containing approval_date column
    
    Returns:
    pd.DataFrame: DataFrame with converted date format
    """
    # Create a copy to avoid modifying the original dataframe
    df_converted = df.copy()
    
    # Convert the date format
    df_converted['approval_date'] = pd.to_datetime(df_converted['approval_date']).dt.strftime('%Y%m%d')
    
    return df_converted

# Alternative function if you want to modify the dataframe in place
def convert_date_format_inplace(df):
    """
    Convert approval_date column from "March 14, 2025" format to YYYYMMDD format in place.
    
    Parameters:
    df (pd.DataFrame): DataFrame containing approval_date column
    """
    df['approval_date'] = pd.to_datetime(df['approval_date']).dt.strftime('%Y%m%d')
    return df