#!/usr/bin/env python3
"""
Standalone script to convert Parquet files to JSON format

This script takes a category name (Electronics, Toys_Games, or Cell_Phones) 
and converts all Parquet files for that category to a single compressed JSON file.

Usage:
    python parquet_to_json_converter_standalone.py Electronics
    python parquet_to_json_converter_standalone.py Toys_Games
    python parquet_to_json_converter_standalone.py Cell_Phones
"""

import os
import sys
import json
import gzip
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import traceback
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

# Configuration
CATEGORIES = {
    "Electronics": {
        "input_dir": "data/raw/Electronics",
        "output_file": "data/raw/Electronics.json.gz"
    },
    "Toys_Games": {
        "input_dir": "data/raw/Toys_Games",
        "output_file": "data/raw/Toys_Games.json.gz"
    },
    "Cell_Phones": {
        "input_dir": "data/raw/Cell_Phones",
        "output_file": "data/raw/Cell_Phones.json.gz"
    }
}

# Set up logging
LOG_FILE = "logs/converter.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def log(message):
    """Log a message to both console and file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"{timestamp} - {message}"
    print(log_message)
    with open(LOG_FILE, "a") as f:
        f.write(log_message + "\n")

class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder that can handle NumPy types"""
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        if isinstance(obj, (np.float64, np.float32, np.float16)):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if pd.isna(obj):
            return None
        return super(NumpyEncoder, self).default(obj)

def convert_category(category):
    """Convert all Parquet files for a category to a single JSON.gz file"""
    if category not in CATEGORIES:
        log(f"Error: Unknown category '{category}'. Must be one of: {', '.join(CATEGORIES.keys())}")
        return False
    
    config = CATEGORIES[category]
    input_dir = config["input_dir"]
    output_file = config["output_file"]
    
    log(f"Starting conversion for category: {category}")
    log(f"Input directory: {input_dir}")
    log(f"Output file: {output_file}")
    
    # Check if input directory exists
    if not os.path.exists(input_dir):
        log(f"Error: Input directory '{input_dir}' does not exist")
        return False
    
    # Find all Parquet files
    parquet_files = sorted(Path(input_dir).glob("*.parquet"))
    if not parquet_files:
        log(f"Error: No Parquet files found in '{input_dir}'")
        return False
    
    log(f"Found {len(parquet_files)} Parquet files to process")
    
    # Process files
    row_count = 0
    
    # Open gzip file directly for writing
    with gzip.open(output_file, "wt", encoding="utf-8") as out_f:
        # Process each Parquet file
        for idx, parquet_file in enumerate(tqdm(parquet_files, desc=f"Converting {category}")):
            try:
                log(f"Processing file {idx+1}/{len(parquet_files)}: {parquet_file}")
                
                # Read the Parquet file
                table = pq.read_table(parquet_file)
                df = table.to_pandas()
                file_rows = len(df)
                log(f"File has {file_rows} rows and {len(df.columns)} columns")
                
                # Convert each row to JSON and write to the compressed output
                for _, row in tqdm(df.iterrows(), total=file_rows, desc=f"File {idx+1}", leave=False):
                    try:
                        # Convert row to dictionary
                        row_dict = row.to_dict()
                        
                        # Convert to JSON using custom encoder
                        json_line = json.dumps(row_dict, cls=NumpyEncoder)
                        out_f.write(json_line + "\n")
                        row_count += 1
                        
                        # Print progress every 10,000 rows
                        if row_count % 10000 == 0:
                            log(f"Processed {row_count} rows so far")
                            
                    except Exception as e:
                        log(f"Error processing row: {str(e)}")
                        if row_count < 5:  # Only log details for first few errors
                            log(traceback.format_exc())
                            
            except Exception as e:
                log(f"Error processing file {parquet_file}: {str(e)}")
                log(traceback.format_exc())
    
    # Get file size
    output_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    log(f"Conversion completed: {row_count} rows written to {output_file} ({output_size_mb:.2f} MB)")
    
    # Create a sample file with first 1000 rows
    sample_file = output_file.replace(".json.gz", "_sample.json")
    log(f"Creating sample file: {sample_file}")
    
    sample_count = 0
    with gzip.open(output_file, "rt", encoding="utf-8") as in_f:
        with open(sample_file, "w", encoding="utf-8") as out_f:
            for line in in_f:
                out_f.write(line)
                sample_count += 1
                if sample_count >= 1000:
                    break
    
    sample_size_kb = os.path.getsize(sample_file) / 1024
    log(f"Created sample file with {sample_count} rows ({sample_size_kb:.2f} KB)")
    
    return True

def main():
    """Main function"""
    if len(sys.argv) != 2:
        log(f"Usage: {sys.argv[0]} <category>")
        log(f"Available categories: {', '.join(CATEGORIES.keys())}")
        return
    
    category = sys.argv[1]
    convert_category(category)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"Unhandled exception: {str(e)}")
        log(traceback.format_exc())