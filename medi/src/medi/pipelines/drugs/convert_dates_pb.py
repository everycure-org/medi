import pandas as pd
from datetime import datetime

def convert_date_format(df):
    """
    Convert approval_date column from DD-MMM-YY format to YYYYMMDD format.
    
    Parameters:
    df (pd.DataFrame): DataFrame containing approval_date column
    
    Returns:
    pd.DataFrame: DataFrame with converted date format
    """
    # Create a copy to avoid modifying the original dataframe
    df_converted = df.copy()
    
    # Convert the date format
    df_converted['approval_date'] = pd.to_datetime(df_converted['approval_date'], 
                                                  format='%d-%b-%y').dt.strftime('%Y%m%d')
    
    return df_converted

# Alternative function if you want to modify the dataframe in place
def convert_date_format_inplace(df):
    """
    Convert approval_date column from DD-MMM-YY format to YYYYMMDD format in place.
    
    Parameters:
    df (pd.DataFrame): DataFrame containing approval_date column
    """
    df['approval_date'] = pd.to_datetime(df['approval_date'], 
                                        format='%d-%b-%y').dt.strftime('%Y%m%d')