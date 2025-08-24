import asyncio
from googletrans import Translator
from tqdm import tqdm
import pandas as pd


def translate_dataframe(df, source_lang='ru', dest_lang='en'):
    """
    Translate both column names and data content from source language to destination language.
    This is a synchronous wrapper function that calls the async implementation.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame to translate
    source_lang : str, default='ru'
        Source language code (Russian by default)
    dest_lang : str, default='en'
        Destination language code (English by default)
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with translated column names and data
    """
    # Run the async function in an event loop
    return asyncio.run(_translate_dataframe_async(df, source_lang, dest_lang))


async def _translate_dataframe_async(df, source_lang='ru', dest_lang='en'):
    """
    Async implementation of the translation function.
    
    Parameters are the same as translate_dataframe.
    """
    # Create a copy of the dataframe to avoid modifying the original
    df_copy = df.copy()
    
    async with Translator() as translator:
        # First translate column names
        print(f"Translating {len(df.columns)} column names from {source_lang} to {dest_lang}...")
        column_mapping = {}
        
        for column in tqdm(df.columns, desc="Translating columns", unit="column"):
            try:
                result = await translator.translate(column, src=source_lang, dest=dest_lang)
                column_mapping[column] = result.text
            except Exception as e:
                print(f"Error translating column '{column}': {e}")
                column_mapping[column] = column  # Keep original on error
        
        # Rename the columns in the DataFrame
        df_copy = df_copy.rename(columns=column_mapping)
        print("Column translation complete!")
        
        # Now translate the text data in each cell
        print(f"Translating data content from {source_lang} to {dest_lang}...")
        
        # Get only the text columns (skip numeric columns)
        text_columns = df_copy.select_dtypes(include=['object']).columns
        
        if len(text_columns) == 0:
            print("No text columns found to translate.")
            return df_copy
            
        # Calculate total cells to translate for progress bar
        total_cells = sum(df_copy[col].notna().sum() for col in text_columns)
        
        # Create a new dataframe for translated content
        translated_df = df_copy.copy()
        
        with tqdm(total=total_cells, desc="Translating cells", unit="cell") as pbar:
            cache = {}
            for col in text_columns:
                for idx in df_copy.index:
                    value = df_copy.at[idx, col]
                    
                    # Only translate if value is a string and not empty
                    if isinstance(value, str) and value.strip():
                        try:
                            # Add small delay to avoid hitting API rate limits
                            
                            if value in cache:
                                result = cache[value]
                            else:
                                await asyncio.sleep(0.1)
                                result = await translator.translate(value, src=source_lang, dest=dest_lang)
                                cache[value]=result
                            translated_df.at[idx, col] = result.text
                        except Exception as e:
                            print(f"Error translating value '{value}': {e}")
                            # Keep original value on error
                    
                    if isinstance(value, str) or pd.notna(value):
                        pbar.update(1)
        
        print("Data translation complete!")
        return translated_df


# For backward compatibility with Kedro pipeline
def translate_dataframe_columns(df, source_lang='ru', dest_lang='en'):
    """
    This function is maintained for backward compatibility.
    Now it fully translates both columns and data.
    """
    return translate_dataframe(df, source_lang, dest_lang)