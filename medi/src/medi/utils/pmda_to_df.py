import pandas as pd
import re
from datetime import datetime
import PyPDF2
import pdfplumber
from typing import List, Dict, Optional

def extract_drug_data_from_pdf(pdf_path: str) -> pd.DataFrame:
    """
    Extract drug approval data from the PDF and return as a pandas DataFrame.
    
    Args:
        pdf_path (str): Path to the PDF file
        
    Returns:
        pd.DataFrame: DataFrame containing the extracted drug data
    """
    
    # Lists to store extracted data
    data_records = []
    
    try:
        # Use pdfplumber for better text extraction
        with pdfplumber.open(pdf_path) as pdf:
            
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                
                if text:
                    # Process each page
                    records = parse_page_text(text, page_num + 1)
                    data_records.extend(records)
                    
    except Exception as e:
        print(f"Error reading PDF with pdfplumber: {e}")
        print("Trying with PyPDF2...")
        
        # Fallback to PyPDF2
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                for page_num, page in enumerate(pdf_reader.pages):
                    text = page.extract_text()
                    if text:
                        records = parse_page_text(text, page_num + 1)
                        data_records.extend(records)
                        
        except Exception as e2:
            print(f"Error reading PDF with PyPDF2: {e2}")
            return pd.DataFrame()
    
    # Convert to DataFrame
    if data_records:
        df = pd.DataFrame(data_records)
        
        # Clean and standardize the data
        df = clean_dataframe(df)
        
        return df
    else:
        print("No data extracted from PDF")
        return pd.DataFrame()

def parse_page_text(text: str, page_num: int) -> List[Dict]:
    """
    Parse text from a single page and extract drug records.
    
    Args:
        text (str): Text content from the page
        page_num (int): Page number
        
    Returns:
        List[Dict]: List of drug records
    """
    records = []
    
    # Split text into lines
    lines = text.split('\n')
    
    # Skip header pages and category description pages
    if any(keyword in text for keyword in ['Review Category Products', 'Review Categories of New Drugs', 'List of Approved']):
        return records
    
    # Pattern to match drug entries
    # Looking for pattern: Category Date No. Brand Name (Company) Approval/Change Active Ingredient
    current_record = {}
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        if not line:
            i += 1
            continue
            
        # Check if this line starts a new drug entry
        # Pattern: Review Category, Date, Number
        date_match = re.search(r'(Jan\.|Feb\.|Mar\.|Apr\.|May|Jun\.|Jul\.|Aug\.|Sep\.|Oct\.|Nov\.|Dec\.)\s+(\d{1,2}),\s+(\d{4})', line)
        
        if date_match:
            # This looks like the start of a new entry
            if current_record and 'approval_date' in current_record:
                records.append(current_record.copy())
            
            current_record = {}
            
            # Extract review category (number at the beginning)
            category_match = re.match(r'^(\d+(?:-\d+)?|AIDS drugs|Oncology drugs|Blood products|Vaccines|RadiopharmaceuticalsRadiopharmaceuticals|In vivo diagnostics|Bio-CMC)', line)
            if category_match:
                current_record['review_category'] = category_match.group(1)
            
            # Extract approval date
            current_record['approval_date'] = f"{date_match.group(1)} {date_match.group(2)}, {date_match.group(3)}"
            
            # Extract entry number
            num_match = re.search(r'\s(\d+)\s', line)
            if num_match:
                current_record['entry_number'] = num_match.group(1)
            
            # Look for brand name in parentheses (company name)
            company_match = re.search(r'\(([^)]+)\)', line)
            if company_match:
                current_record['company'] = company_match.group(1)
            
            # Extract brand name (text before company name)
            brand_match = re.search(r'\d+\s+(.+?)\s*\([^)]+\)', line)
            if brand_match:
                current_record['brand_name'] = brand_match.group(1).strip()
            
            # Look for approval type
            if 'Approval' in line:
                current_record['approval_type'] = 'Approval'
            elif 'Change' in line:
                current_record['approval_type'] = 'Change'
            
        else:
            # This might be a continuation line or active ingredient
            if current_record:
                # Check if this contains active ingredient information
                if any(keyword in line.lower() for keyword in ['hydrate', 'sodium', 'chloride', 'recombination', 'mg', 'acid']):
                    if 'active_ingredient' not in current_record:
                        current_record['active_ingredient'] = line
                    else:
                        current_record['active_ingredient'] += ' ' + line
                
                # Check for additional brand names
                elif '(' in line and ')' in line and 'active_ingredient' not in current_record:
                    # This might be additional brand name info
                    if 'brand_name' in current_record:
                        current_record['brand_name'] += ' ' + line
                    
        i += 1
    
    # Don't forget the last record
    if current_record and 'approval_date' in current_record:
        records.append(current_record)
    
    return records

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize the DataFrame.
    
    Args:
        df (pd.DataFrame): Raw DataFrame
        
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    
    # Convert approval_date to datetime
    if 'approval_date' in df.columns:
        df['approval_date'] = pd.to_datetime(df['approval_date'], errors='coerce')
    
    # Clean text columns
    text_columns = ['brand_name', 'company', 'active_ingredient']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            # Remove excessive whitespace
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
    
    # Convert entry_number to numeric
    if 'entry_number' in df.columns:
        df['entry_number'] = pd.to_numeric(df['entry_number'], errors='coerce')
    
    # Reorder columns
    column_order = ['review_category', 'approval_date', 'entry_number', 'brand_name', 
                   'company', 'approval_type', 'active_ingredient']
    
    # Only include columns that exist in the DataFrame
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]
    
    # Sort by approval date and entry number
    df = df.sort_values(['approval_date', 'entry_number'], na_position='last')
    
    # Reset index
    df = df.reset_index(drop=True)
    
    return df

def save_to_excel(df: pd.DataFrame, output_path: str = 'drug_approvals.xlsx'):
    """
    Save DataFrame to Excel file with formatting.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        output_path (str): Output file path
    """
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Drug Approvals', index=False)
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Drug Approvals']
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"Data saved to {output_path}")
        
    except Exception as e:
        print(f"Error saving to Excel: {e}")
        # Fallback to CSV
        csv_path = output_path.replace('.xlsx', '.csv')
        df.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path} instead")

# Main execution
if __name__ == "__main__":
    # Specify the path to your PDF file
    pdf_file_path = "data/drugs/01_raw/pmda_approvals.pdf"  # Update this path
    
    print("Extracting data from PDF...")
    df = extract_drug_data_from_pdf(pdf_file_path)
    
    if not df.empty:
        print(f"Successfully extracted {len(df)} records")
        print("\nFirst few records:")
        print(df.head())
        
        print("\nDataFrame info:")
        print(df.info())
        
        # Save to Excel
        save_to_excel(df, 'drug_approvals_extracted.xlsx')
        
        # Also save to CSV
        df.to_csv('drug_approvals_extracted.csv', index=False)
        print("Data also saved to drug_approvals_extracted.csv")
        
    else:
        print("No data was extracted. Please check the PDF file and try again.")