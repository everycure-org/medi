import pandas as pd
from openai import OpenAI
import os
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from tqdm import tqdm 


def extract_outputs_and_prompts(data_dict):
    output_cols = []
    prompts = []
    model = []
    temp = []
    input_cols = []
    # Extract output_col and prompt for each tag
    for tag_name, tag_info in data_dict.items():
        output_cols.append(tag_info['output_col'])
        prompts.append(tag_info['model_params']['prompt'])
        model.append(tag_info['model_params']['model'])
        temp.append(tag_info['model_params']['temperature'])
    return output_cols, prompts, model, temp

def add_tags(in_df: pd.DataFrame, tags: dict, labels_col: str) -> pd.DataFrame: 
    df = in_df.copy()
    feature_names, feature_descriptions, model, temp= extract_outputs_and_prompts(tags)
    for feature_name, feature_description, model, temp in tqdm(zip(feature_names, feature_descriptions, model, temp)):
        df = generate_features(input_df=df, new_feature_name=feature_name, feature_description=feature_description, label_colname=labels_col, model_name=model, temp=temp)
    print(feature_names)
    print(feature_descriptions)
    return df


def single_openai_prompt(prompt, model="gpt-4o", temperature=0.1):
    """
    Run a single prompt using OpenAI with LangChain
    
    Args:
        prompt (str): The prompt/question to send to the model
        model (str): OpenAI model to use (default: gpt-3.5-turbo)
        temperature (float): Temperature for response randomness (0-1)
        api_key (str): OpenAI API key (if not set as environment variable)
    
    Returns:
        str: The model's response
    """
    client = OpenAI()
    # Initialize the ChatOpenAI model
    llm = ChatOpenAI(
        model=model,
        temperature=temperature
    )
    #print(prompt)
    # Create a message and invoke the model
    try:
        response = client.responses.create(
            model=model,
            input=prompt
        )
    except Exception as e:
        print(e)
        return ['Error']
    
    print(f"response:{response.output_text}")
    return response.output_text


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
    model = ChatOpenAI(model=model_name, temperature=temp, max_retries=3, model_kwargs={"response_format": {"type": "json_object"}})
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
    response = chain.batch(list(df[label_colname]), config={"max_concurrency": 80})
    # for i, r in enumerate(response):
    #     if not r:
    #         response[i] = False
    feature_df = pd.DataFrame([json.loads(r.content) for r in response])
    df.update(feature_df)
    return df