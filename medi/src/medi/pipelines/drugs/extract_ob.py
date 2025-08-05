# nodes.py
import zipfile
import tempfile
import subprocess
import pandas as pd
from pathlib import Path

def extract_and_process_fda_zip(zip_url: str) -> pd.DataFrame:
    """
    Download FDA zip file using curl and extract products.txt as DataFrame
    
    Args:
        zip_url: URL to the FDA zip file
        
    Returns:
        pd.DataFrame: Contents of products.txt file
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        zip_path = temp_dir_path / "fda_data.zip"
        
        # Download the zip file using curl
        curl_command = [
            'curl',
            '-L',  # Follow redirects
            '-o', str(zip_path),  # Output file
            '-A', 'Mozilla/5.0 (compatible; DataProcessor/1.0)',  # User agent
            zip_url
        ]
        
        result = subprocess.run(curl_command, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"curl failed: {result.stderr}")
        
        # Extract zip file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            
            # Find products.txt file (should be in EOBZIP_2025_06 folder)
            products_file = list(Path(temp_dir).glob("**/products.txt"))
            
            if not products_file:
                raise FileNotFoundError("products.txt not found in the zip archive")
            
            # Read the products.txt file (assuming tab-delimited)
            df = pd.read_csv(products_file[0], sep='\t', dtype=str, low_memory=False)
            
        return df