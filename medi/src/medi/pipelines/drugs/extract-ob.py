import pandas as pd 
import tempfile
import requests
from pathlib import Path
import zipfile 

def extract_and_process_fda_zip(zip_url: str) -> pd.DataFrame:
    """
    Download FDA zip file and extract products.txt as DataFrame
    
    Args:
        zip_url: URL to the FDA zip file
        
    Returns:
        pd.DataFrame: Contents of products.txt file
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        zip_path = temp_dir_path / "fda_data.zip"
        
        # Download the zip file
        response = requests.get(zip_url, stream=True)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Extract zip file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            
            # Find products.txt file (should be in EOBZIP_2025_06 folder)
            products_file = list(Path(temp_dir).glob("**/products.txt"))
            
            if not products_file:
                raise FileNotFoundError("products.txt not found in the zip archive")
            
            # Read the products.txt file (assuming tab-delimited)
            df = pd.read_csv(products_file[0], sep='~', dtype=str, low_memory=False)
            
        return df