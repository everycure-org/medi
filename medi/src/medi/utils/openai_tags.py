import pandas as pd
from openai import OpenAI
import os
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from tqdm import tqdm 

def extract_outputs_and_prompts(data_dict):
    output_cols = []
    prompts = []
    model = []
    temp = []
    input_cols = []
    # Extract output_col and prompt for each tag
    for tag_name, tag_info in data_dict.items():
        input_cols.append(tag_info['input_col'])
        output_cols.append(tag_info['output_col'])
        prompts.append(tag_info['model_params']['prompt'])
        model.append(tag_info['model_params']['model'])
        temp.append(tag_info['model_params']['temperature'])
    return input_cols, output_cols, prompts, model, temp

def add_tags(in_df: pd.DataFrame, tags: dict) -> pd.DataFrame: 
    df = in_df.copy()
    input_cols, feature_names, feature_descriptions, model, temp= extract_outputs_and_prompts(tags)
    for label_col, feature_name, feature_description, model, temp in tqdm(zip(input_cols, feature_names, feature_descriptions, model, temp)):
        if feature_name not in df.columns:
            df = generate_features(df, feature_name, feature_description, label_col, model, temp)
            print(f"{feature_name} generated")
        else:
            print(f"{feature_name} already exists")
    print(feature_names)
    print(feature_descriptions)
    return df


def generate_features(input_df: pd.DataFrame, new_feature_name: str, feature_description: str, label_colname: str, model_name: str, temp: float):
    """
    Generate new features for a pandas DataFrame using specified model
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The dataframe containing the text to analyze
    text_column : str
        The name of the column containing text to analyze
    new_feature_names : list
        List of names for the new feature columns to be created
    feature_descriptions : list
        List of descriptions for what each feature should represent
    
    Returns:
    --------
    pandas.DataFrame
        The original dataframe with new feature columns added
    """
    df = input_df.copy()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    model = ChatOpenAI(name=model_name, temperature=temp, max_retries=3, model_kwargs={"response_format": {"type": "json_object"}})
    df[new_feature_name] = None 
    prompt = ChatPromptTemplate.from_messages([
        (
            "system", 
            f"""You are a medical doctor trying to categorize a list of drugs. 
            For each drug name, extract the following features: 
            "{new_feature_name}: {feature_description}".
            Return ONLY a JSON object with the feature names as keys and the extracted values. 
            No explanations or other text."""
        ),
        (
            "user", 
            "drug to analyze: {drug_name}"
        )
    ])
    chain = prompt | model
    response = chain.batch(list(df[label_colname]), config={"max_concurrency": 50})
    for i, r in enumerate(response):
        if not r:
            response[i] = False
    feature_df = pd.DataFrame([json.loads(r.content) for r in response])
    df.update(feature_df)
    return df