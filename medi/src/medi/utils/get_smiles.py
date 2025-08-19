from tqdm import tqdm
import pandas as pd
import requests
from typing import Optional

def string_to_list(input_string):
    """
    Convert a string representation of a list into an actual Python list of strings.
    
    Args:
        input_string (str): String representation of a list (e.g., "['item1','item2','item3']")
        
    Returns:
        list: A list of strings
    
    Example:
        >>> string_to_list("['item1','item2','item3']")
        ['item1', 'item2', 'item3']
    """
    cleaned_string = input_string.strip('[]')
    items = [item.strip().strip("'").strip('"') for item in cleaned_string.split(',')]
    items = [item for item in items if item]
    return items

def extract_pubchem_id(identifier):
    """
    Extract the numeric ID from PubChem identifier strings.
    
    Args:
        identifier (str): String containing PubChem identifier
            e.g., "PUBCHEM:1234" or "PUBCHEM.COMPOUND:1234" or "1234"
            
    Returns:
        str: The extracted ID or original string if no pattern is matched
    
    Examples:
        >>> extract_pubchem_id("PUBCHEM:1234")
        "1234"
        >>> extract_pubchem_id("PUBCHEM.COMPOUND:1234")
        "1234"
        >>> extract_pubchem_id("1234")
        "1234"
    """
    if "PUBCHEM.COMPOUND:" in identifier:
        return identifier.split("PUBCHEM.COMPOUND:")[-1]
    elif "PUBCHEM:" in identifier:
        return identifier.split("PUBCHEM:")[-1]
    return identifier



def add_SMILES_strings(drug_list: pd.DataFrame) -> pd.DataFrame:
    smiles = []
    for idx, row in tqdm(drug_list.iterrows(), total=len(drug_list)):
        #print(row['curie'])
        identifier = row['corrected_curie_norm']
        alt_ids = string_to_list(row['alternate_ids'])
        #print((alt_ids))
        if "PUBCHEM" in identifier:
            pc_id = int(extract_pubchem_id(identifier))
            smiles.append(get_smiles_from_pubchem(pc_id))
        else:
            found_id = False
            for item in alt_ids:
                if "PUBCHEM" in item:
                    found_id = True
                    pc_id = int(extract_pubchem_id(item))
                    smiles.append(get_smiles_from_pubchem(pc_id))
                    break
            if not found_id:
                smiles.append("")
    drug_list['smiles']=smiles
    return drug_list


def get_smiles_from_pubchem(pubchem_id: int) -> Optional[str]:
    """
    Retrieve the SMILES string for a chemical compound using its PubChem ID (CID).
    
    Args:
        pubchem_id (int): The PubChem Compound ID (CID)
    
    Returns:
        Optional[str]: The SMILES string if found, None if not found or error occurs
        
    Raises:
        ValueError: If the pubchem_id is not a positive integer
        requests.RequestException: If there's an error with the API request
    """
    if not isinstance(pubchem_id, int) or pubchem_id <= 0:
        raise ValueError("PubChem ID must be a positive integer")

    # PubChem REST API endpoint
    base_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    endpoint = f"{base_url}/compound/cid/{pubchem_id}/property/IsomericSMILES/JSON"
    try:
        # Make the API request
        response = requests.get(endpoint)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse the JSON response
        data = response.json()
        
        # Extract the SMILES string
        smiles = data["PropertyTable"]["Properties"][0]["SMILES"]
        return smiles
        
    except requests.RequestException as e:
        print("API request error.")
        return None