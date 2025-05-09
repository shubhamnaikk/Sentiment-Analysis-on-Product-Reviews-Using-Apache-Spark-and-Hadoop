import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import json
import os

# Try to read just one Parquet file
file_path = "data/raw/Electronics/00000-of-00010.parquet"
output_file = "data/raw/test_output.json"

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

print(f"Reading Parquet file: {file_path}")
try:
    # Read the parquet file
    table = pq.read_table(file_path)
    print(f"Successfully read Parquet file. Table info: {table.shape}")
    
    # Convert to pandas DataFrame
    df = table.to_pandas()
    print(f"Converted to DataFrame. Shape: {df.shape}")
    
    # Sample some data
    print("First few column names:")
    print(df.columns.tolist()[:10])  # Print first 10 column names
    
    # Count rows
    print(f"Number of rows: {len(df)}")
    
    # Get a sample row to inspect problematic fields
    sample_row = df.iloc[0].to_dict()
    print("\nSample row field types:")
    for k, v in sample_row.items():
        print(f"{k}: {type(v)}")
    
    # Try to write a few rows to JSON
    print("\nWriting 10 rows to JSON...")
    with open(output_file, 'w', encoding='utf-8') as out_f:
        for idx, row in df.head(10).iterrows():
            try:
                # Convert row to dict
                row_dict = row.to_dict()
                
                # Convert to JSON using custom encoder and write to file
                json_line = json.dumps(row_dict, cls=NumpyEncoder)
                out_f.write(json_line + '\n')
                
                if idx == 0:  # Print the first JSON line for inspection
                    print(f"\nFirst row serialized successfully")
            except Exception as json_err:
                print(f"Error processing row {idx}: {json_err}")
    
    print(f"\nFinished writing sample to {output_file}")
    file_size = os.path.getsize(output_file)
    print(f"Output file size: {file_size} bytes")
    
    # Check if the output is valid
    if file_size > 0:
        print("\nSample output content:")
        with open(output_file, 'r') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[:2]):  # Print first 2 lines
                print(f"Line {i+1} (first 200 chars): {line[:200]}...")
    
except Exception as e:
    import traceback
    print(f"Error: {str(e)}")
    print(traceback.format_exc())