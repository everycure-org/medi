import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import ast
import re
from urllib.parse import quote


def get_atc_from_rxnorm(rxnorm_id):
    """Get ATC code from RxNorm ID using the RxNav API"""
    try:
        # First, validate if the input is a valid RxNorm ID (numeric)
        if not rxnorm_id.isdigit():
            return None
            
        # Call the RxNav API to get ATC codes
        response = requests.get(f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxnorm_id}/property?propName=ATC")
        
        if response.status_code == 200:
            data = response.json()
            prop_concept_group = data.get('propConceptGroup', {})
            if prop_concept_group:
                prop_concepts = prop_concept_group.get('propConcept', [])
                atc_codes = [prop['propValue'] for prop in prop_concepts if prop.get('propName') == 'ATC']
                return atc_codes if atc_codes else None
        return None
    except Exception as e:
        print(f"Error getting ATC from RxNorm for {rxnorm_id}: {str(e)}")
        return None

def get_atc_from_chebi(chebi_id):
    """Get ATC code from ChEBI ID using the ChEBI API"""
    try:
        # Remove the 'CHEBI:' prefix if present
        if 'CHEBI:' in chebi_id:
            chebi_id = chebi_id.split(':')[1]
        
        # Call the ChEBI API
        response = requests.get(f"https://www.ebi.ac.uk/webservices/chebi/2.0/test/getCompleteEntity?chebiId={chebi_id}")
        
        if response.status_code == 200:
            # Parse the XML response
            root = ET.fromstring(response.content)
            
            # Check for ATC codes in the cross-references
            atc_classifications = []
            for ref in root.findall('.//DatabaseAccession'):
                if ref.find('..//Data').text == 'ATC':
                    atc_classifications.append(ref.text)
            
            return atc_classifications if atc_classifications else None
        return None
    except Exception as e:
        print(f"Error getting ATC from ChEBI for {chebi_id}: {str(e)}")
        return None

def get_atc_from_chembl(chembl_id):
    """Get ATC code from ChEMBL ID using the ChEMBL API"""
    try:
        # Remove the 'CHEMBL.COMPOUND:' prefix if present
        if 'CHEMBL.COMPOUND:' in chembl_id:
            chembl_id = chembl_id.split(':')[1]
        
        # Call the ChEMBL API
        response = requests.get(f"https://www.ebi.ac.uk/chembl/api/data/molecule/{chembl_id}.json")
        
        if response.status_code == 200:
            data = response.json()
            atc_classifications = data.get('atc_classifications', [])
            return atc_classifications if atc_classifications else None
        return None
    except Exception as e:
        print(f"Error getting ATC from ChEMBL for {chembl_id}: {str(e)}")
        return None

def get_atc_from_pubchem(pubchem_id):
    """Get ATC code from PubChem ID using PubChem PUG REST API"""
    try:
        # Remove the 'PUBCHEM.COMPOUND:' prefix if present
        if 'PUBCHEM.COMPOUND:' in pubchem_id:
            pubchem_id = pubchem_id.split(':')[1]
            
        # The PubChem API doesn't directly provide ATC codes, so we'll use the classification browser
        response = requests.get(f"https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{pubchem_id}/JSON")
        
        if response.status_code == 200:
            data = response.json()
            sections = data.get('Record', {}).get('Section', [])
            
            # Look for ATC codes in the classifications
            for section in sections:
                if section.get('TOCHeading') == 'Classification':
                    subsections = section.get('Section', [])
                    for subsection in subsections:
                        if 'ATC' in subsection.get('TOCHeading', ''):
                            information = subsection.get('Information', [])
                            for info in information:
                                value = info.get('Value', {}).get('StringWithMarkup', [])
                                atc_codes = []
                                for val in value:
                                    atc_match = re.search(r'[A-Z]\d\d[A-Z][A-Z]\d\d', val.get('String', ''))
                                    if atc_match:
                                        atc_codes.append(atc_match.group(0))
                                if atc_codes:
                                    return atc_codes
        return None
    except Exception as e:
        print(f"Error getting ATC from PubChem for {pubchem_id}: {str(e)}")
        return None

def get_atc_from_drugcentral(drugcentral_id):
    """Get ATC code from DrugCentral ID using the DrugCentral API"""
    try:
        # Extract the numeric ID if it has a prefix
        if 'DrugCentral:' in drugcentral_id:
            drugcentral_id = drugcentral_id.split(':')[1]
            
        # Call the DrugCentral API
        response = requests.get(f"https://drugcentral.org/api/drugcentral/structures?q={drugcentral_id}")
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                drug_data = data[0]
                atc_codes = []
                for annotation in drug_data.get('annotations', []):
                    if annotation.get('type') == 'ATC':
                        atc_codes.append(annotation.get('value'))
                return atc_codes if atc_codes else None
        return None
    except Exception as e:
        print(f"Error getting ATC from DrugCentral for {drugcentral_id}: {str(e)}")
        return None

def get_atc_from_whocc(drug_name):
    """Get ATC code from WHO Collaborating Centre for Drug Statistics Methodology"""
    try:
        # Encode the drug name for URL
        encoded_name = quote(drug_name)
        
        # Search the WHO ATC database
        response = requests.get(f"https://www.whocc.no/atc_ddd_index/?name={encoded_name}")
        
        if response.status_code == 200:
            # Parse HTML response to extract ATC codes
            # This is a placeholder as proper HTML parsing would be needed
            atc_matches = re.findall(r'[A-Z]\d\d[A-Z][A-Z]\d\d', response.text)
            return atc_matches if atc_matches else None
        return None
    except Exception as e:
        print(f"Error getting ATC from WHO for {drug_name}: {str(e)}")
        return None

def extract_id_from_curie(id_string, prefix):
    """Extract specific ID from a CURIE string"""
    for item in id_string.split(','):
        item = item.strip()
        if item.startswith(prefix):
            return item
    return None

def get_chebi_drugcentral_xrefs(chebi_id):
    """
    Get DrugCentral XREFs from a CHEBI ID using the CHEBI API or OLS
    
    Parameters:
    chebi_id (str): CHEBI ID (format: CHEBI:XXXXX)
    
    Returns:
    list: List of DrugCentral IDs referenced by this CHEBI entry
    """
    try:
        # Extract the numeric part if needed
        if ':' in chebi_id:
            chebi_num = chebi_id.split(':')[1]
        else:
            chebi_num = chebi_id
            
        # Use the EBI OLS API to get cross-references
        response = requests.get(f"https://www.ebi.ac.uk/ols/api/ontologies/chebi/terms/http%253A%252F%252Fpurl.obolibrary.org%252Fobo%252FCHEBI_{chebi_num}")
        
        if response.status_code == 200:
            data = response.json()
            xrefs = data.get('annotation', {}).get('database_cross_reference', [])
            
            # Filter for DrugCentral references
            drugcentral_ids = []
            for xref in xrefs:
                if isinstance(xref, str) and xref.startswith('DrugCentral:'):
                    drugcentral_ids.append(xref)
            
            return drugcentral_ids
        return []
    except Exception as e:
        print(f"Error getting DrugCentral XREFs from CHEBI for {chebi_id}: {str(e)}")
        return []

def get_atc_for_row(row, dict):
    """Process a single row to find ATC code"""
    colname = "corrected_curie_norm"

    # if curie is in the dict
    if row[colname] in dict:
        return [dict[row[colname]]]

    # Convert string representation of list to actual list if needed
    if isinstance(row['alternate_ids'], str):
        try:
            alt_ids = ast.literal_eval(row['alternate_ids'])
        except (SyntaxError, ValueError):
            alt_ids = [id.strip() for id in row['alternate_ids'].strip('[]').split(',')]
    else:
        alt_ids = row['alternate_ids']

    # Add the curie to the list of IDs if not already there
    if row[colname] not in alt_ids:
        alt_ids.append(row[colname])
    
    # Try each source in order of reliability/accessibility
    for id_item in alt_ids:
        id_item = id_item.strip("'\" ")

        if 'CHEBI' in id_item:
            atc = get_atc_from_chebi(id_item)
            if atc:
                return atc
        
        # Try ChEMBL
        if 'CHEMBL.COMPOUND:' in id_item:
            atc = get_atc_from_chembl(id_item)
            if atc:
                return atc
        
        # Try PubChem
        if 'PUBCHEM.COMPOUND:' in id_item:
            atc = get_atc_from_pubchem(id_item)
            if atc:
                return atc
        
        # Try DrugCentral
        if 'DrugCentral:' in id_item:
            atc = get_atc_from_drugcentral(id_item)
            if atc:
                return atc
    
    # If no ATC found yet, check if there are CHEBI IDs and try to get DrugCentral XREFs
    chebi_ids = [id_item.strip("'\" ") for id_item in alt_ids if 'CHEBI:' in id_item]
    for chebi_id in chebi_ids:
        # Get DrugCentral XREFs from CHEBI
        drugcentral_refs = get_chebi_drugcentral_xrefs(chebi_id)
        
        # Try to get ATC codes from each DrugCentral reference
        for dc_ref in drugcentral_refs:
            atc = get_atc_from_drugcentral(dc_ref)
            if atc:
                return atc
    
    # If we have a name column, try WHO CC as last resort
    if 'name' in row and row['name']:
        atc = get_atc_from_whocc(row['name'])
        if atc:
            return atc
    
    return None


def get_atc_codes_for_dataframe(df, atc_standard, max_workers=5):
    """
    Get ATC classifications for all drugs in a dataframe
    
    Parameters:
    df (pandas.DataFrame): DataFrame with 'curie' and 'alternate_ids' columns
    max_workers (int): Maximum number of parallel workers for API calls
    
    Returns:
    pandas.DataFrame: Original dataframe with new 'atc_codes' column
    """
    atc_standard_dict_id_to_code = {}
    for idx, row in tqdm(atc_standard.iterrows(), total=len(atc_standard), desc="building ATC dictionary of IDs"):
        if row['normalized_id'] != "E":
            atc_standard_dict_id_to_code[row['normalized_id']]=row['Class ID'].replace("http://purl.bioontology.org/ontology/ATC/","")
    # Make a copy to avoid modifying the original
    result_df = df.copy()
    
    # Process rows in parallel for efficiency
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_index = {executor.submit(get_atc_for_row, row, atc_standard_dict_id_to_code): i 
                          for i, row in df.iterrows()}
        
        # Collect results
        atc_codes = [None] * len(df)
        for future in tqdm(future_to_index):
            index = future_to_index[future]
            try:
                atc_codes[index] = future.result()
            except Exception as e:
                print(f"Error processing row {index}: {str(e)}")
                atc_codes[index] = None
    
    # Add results to dataframe
    result_df['atc_codes'] = atc_codes
    
     # Function to break down ATC code into levels
    def break_down_atc(atc_code):
        if not atc_code:
            return None, None, None, None, None
        
        # Level 1: Anatomical main group (first character)
        level1 = atc_code[0] if len(atc_code) >= 1 else None
        
        # Level 2: Therapeutic subgroup (first 3 characters)
        level2 = atc_code[:3] if len(atc_code) >= 3 else None
        
        # Level 3: Pharmacological subgroup (first 4 characters)
        level3 = atc_code[:4] if len(atc_code) >= 4 else None
        
        # Level 4: Chemical subgroup (first 5 characters)
        level4 = atc_code[:5] if len(atc_code) >= 5 else None
        
        # Level 5: Chemical substance (all 7 characters)
        level5 = atc_code if len(atc_code) == 7 else None
        
        return level1, level2, level3, level4, level5
    
    # Apply the top-level ATC code function
    def get_top_atc(atc_codes):
        return atc_codes[0] if isinstance(atc_codes, list) and atc_codes else None
    
    result_df['atc_main'] = result_df['atc_codes'].apply(get_top_atc)
    
    # Break down the main ATC code into levels
    result_df[['atc_level1', 'atc_level2', 'atc_level3', 'atc_level4', 'atc_level5']] = pd.DataFrame(
        result_df['atc_main'].apply(break_down_atc).tolist(), 
        index=result_df.index
    )
    
    return result_df