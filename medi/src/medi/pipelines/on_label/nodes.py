"""
This is a boilerplate pipeline 'on_label'
generated using Kedro 0.19.14
"""

from matplotlib_venn import venn3
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET
import re, os
import zipfile
import itertext
from tqdm import tqdm 




################################
### EXTRACTION #################
################################
def extract_contraindications(xml_file_path):
    """
    Extract contraindications from a Structured Product Label XML file.
    
    Args:
        xml_file_path (str): Path to the SPL XML file
        
    Returns:
        dict: Dictionary containing contraindications text and metadata
    """
    # Define the namespace used in SPL files
    namespaces = {
        'v3': 'urn:hl7-org:v3',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
    }
    
    try:
        # Parse the XML file
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        # Initialize results dictionary
        results = {
            'contraindications': None,
            'section_code': None,
            'section_id': None,
            'metadata': {}
        }
        
        # Find the contraindications section
        # SPL uses code 34070-3 for contraindications
        for section in root.findall('.//v3:section', namespaces):
            code_element = section.find('.//v3:code', namespaces)
            if code_element is not None:
                code = code_element.get('code')
                if code == '34070-3':  # LOINC code for contraindications
                    # Extract section ID if present
                    results['section_id'] = section.get('ID', '')
                    
                    # Extract section code details
                    results['section_code'] = {
                        'code': code,
                        'codeSystem': code_element.get('codeSystem', ''),
                        'displayName': code_element.get('displayName', '')
                    }
                    
                    # Extract the text content
                    text_element = section.find('.//v3:text', namespaces)
                    if text_element is not None:
                        # Convert the text element to string, preserving internal markup
                        text_content = ET.tostring(text_element, encoding='unicode', method='xml')
                        
                        # Clean up the text content
                        # Remove XML tags while preserving content
                        clean_text = re.sub(r'<[^>]+>', ' ', text_content)
                        # Remove extra whitespace
                        clean_text = ' '.join(clean_text.split())
                        
                        results['contraindications'] = clean_text
                    
                    # Extract any additional metadata
                    title_element = section.find('.//v3:title', namespaces)
                    if title_element is not None:
                        results['metadata']['title'] = title_element.text
                    
                    break  # Stop after finding the contraindications section
        
        return results
    
    except ET.ParseError as e:
        raise Exception(f"Error parsing XML file: {str(e)}")
    except Exception as e:
        raise Exception(f"Error processing SPL file: {str(e)}")

def unzip_file(zip_path, extract_to_folder):
    if not os.path.isfile(zip_path):
        raise FileNotFoundError(f"The file {zip_path} does not exist.")
    os.makedirs(extract_to_folder, exist_ok=True) 
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
       #print(f"Extracted all contents to {extract_to_folder}")

def extract_active_ingredient(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    ns = {'fda': 'urn:hl7-org:v3'}
    active_ingredients = []
    for ingredient in root.findall(".//fda:activeMoiety/fda:name", ns):
        active_ingredients.append(ingredient.text)
    return active_ingredients

def getIndications(xmlfilepath):
    tree = ET.parse(xmlfilepath)
    root = tree.getroot()
    ns = {'hl7': 'urn:hl7-org:v3'}
    sections = root.findall('.//hl7:section', namespaces=ns)
    for section in sections:
        codeSection = section.find('.//hl7:code', namespaces=ns)
        code = codeSection.get('code') if codeSection is not None else "no code"
        if code == "34067-9":
            text_elem = section.find('.//hl7:text', namespaces=ns)
            try:
                text_content = ''.join(text_elem.itertext()).strip()
            except:
                print('text_elem was empty')
                return ""
            return strip_spaces(text_content.strip(string.whitespace.replace(" ", "")))
        else:
            text_elem = None    
    return None

def strip_spaces(myString):
    _RE_COMBINE_WHITESPACE = re.compile(r"(?a:\s+)")
    _RE_STRIP_WHITESPACE = re.compile(r"(?a:^\s+|\s+$)")
    myString = _RE_COMBINE_WHITESPACE.sub(" ", myString)
    myString = _RE_STRIP_WHITESPACE.sub("", myString)
    return myString



def mine_contraindications(dir: str) -> pd.DataFrame:
    contraindications_list = []
    ingredients_list = []
    counts = 0
    foundCounts = 0
    notFoundCounts = 0
    dirs = []
    # TODO: automatically find, download, unzip all of the dailymed folders
    labelFolders = ["prescription_1/", "prescription_2/", "prescription_3/", "prescription_4/", "prescription_5/"]
    
    for label in labelFolders:
        dirs.append(dir+label)

    for directory in (dirs):
        for files in tqdm(os.listdir(directory), desc=f"reading directory {directory}"):
            if files.endswith(".zip"):
                fpath = directory + files
                fileRoot = files.replace(".zip","")
                dest = directory + fileRoot
                try:
                    unzip_file(fpath,dest)
                except:
                    #print("failed to unzip file ", fpath)
                    continue
                xmlfile=""
                for contents in os.listdir(dest):
                    if contents.endswith(".xml"):
                        xmlfile=contents.replace("._","")
                xmlfilepath = dest+"/"+xmlfile
                contraindications = extract_contraindications(xmlfilepath)
                active_ingredients = extract_active_ingredient(xmlfilepath)
                for ind, item in enumerate(active_ingredients):
                    active_ingredients[ind]=item.upper()
                ingredients_list.append(set(active_ingredients))
                if contraindications is not None:
                    contraindications_list.append(contraindications['contraindications'])
                    foundCounts += 1
                    #print(foundCounts, " indications successfully found so far")
                else:
                    notFoundCounts += 1
                    #print(notFoundCounts, " indications not found so far, failed to find for ", files)
                    contraindications_list.append("")
                counts +=1
        
    print("finished ingesting indications")
    data = pd.DataFrame({'active ingredient':ingredients_list, 'contraindications':contraindications_list})
    return data


