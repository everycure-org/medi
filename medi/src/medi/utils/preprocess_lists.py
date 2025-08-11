import pandas as pd 
import requests
import aiohttp
from tqdm import tqdm 
from datetime import datetime
import re 

def preprocess_ema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Args:
        rawdata(pd.DataFrame): raw EMA data
    Returns:
        pd.DataFrame: a raw list of all of the drugs in the EMA book with some processing done to remove problematic rows.
    """

    #Fully Authorized Human Medicines Here
    mask = df['Category']=='Human'
    df = df[mask]
    mask = (df['Medicine status']=="Authorised") 
    df_auth = df[mask]

    # for idx, row in df_auth.iterrows():
    #     print(row['International non-proprietary name (INN) / common name'])
    #     df_auth[idx, 'International non-proprietary name (INN) / common name']=(row['International non-proprietary name (INN) / common name']).replace(";", )


    # # Positive opinions here - maybe use later
    # mask=df['Medicine status']=='Opinion' 
    # df_opinion = df[mask]
    # mask = df_opinion['Opinion status']=='Positive'
    # df_positive_opinion = df_opinion[mask]

    
    return df_auth

def preprocess_pmda(rawdata:pd.DataFrame) -> pd.DataFrame:
    """
    Args:
        rawdata(pd.DataFrame): raw PMDA data
    Returns:
        pd.DataFrame: processed PMDA data based on existing problems.
    """
    new_names = []
    name_label = "Active Ingredient (underlined: new active ingredient)"
    for idx, row in tqdm(rawdata.iterrows(), total=len(rawdata), desc="cleaning PMDA list"):
        item = row[name_label]
        try:
            item = item.upper()
            item = item.strip().replace("A COMBINATION DRUG OF", "").strip(' ')
            if "(" in item:
                item = item.replace("(1)","").replace("(2)","").replace("(3)","").replace("(4)","").replace("(5)","").replace("(6)","").replace("(7)","").replace("(8)","")\
                .replace("(9)","").replace("(10)","").replace("(11)","").replace("(12)","").replace("(13)","").replace("(14)","").replace("(15)","").replace("(16)","")\
                .replace("(17)","").replace("(18)","").replace("(19)","").replace("(20)","").replace("(20)","").replace("(21)","").replace("1)","").replace("2)","").replace("5)","")

            item = item.replace("1)", "").replace("2)", "").replace("5)","")

            item = item.replace("\n", " ")

            if type(item)!=float and (("," in item) or ("/" in item) or (" AND " in item)):
                item= item.replace(",","; ").replace(" AND ", "; ").replace("/","; ").replace(";;",";").replace(";  ", "; ").replace("  ;", ";").replace(" ;",";").strip()
            new_names.append(item)

        except:
            print("encountered problem with ", item)
            new_names.append("error")
    rawdata[name_label] = new_names
    return rawdata


def reformat_dates_ema(df: pd.DataFrame) -> pd.DataFrame:
    df['approval_date']=pd.to_datetime(df['approval_date']).dt.strftime('%Y%m%d')
    return df

