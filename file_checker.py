
#!/usr/bin/env python3
"""
JSON File Checker
This script examines the JSON files in your project to help determine their format and content.
"""

import os
import sys
import json
import glob

def check_file(file_path):
    """Check the format and content of a JSON file."""
    print(f"\nChecking file: {file_path}")
    print(f"File size: {os.path.getsize(file_path) / (1024*1024):.2f} MB")
    
    # Try to determine the format
    try:
        with open(file_path, 'r') as f:
            # Check first few characters
            first_chars = f.read(10)
            f.seek(0)
            
            # Check if it starts with [ or { (regular JSON)
            if first_chars.strip().startswith('[') or first_chars.strip().startswith('{'):
                print("Format appears to be regular JSON (starts with [ or {)")
                
                # Try to load as regular JSON
                try:
                    data = json.load(f)
                    if isinstance(data, list):
                        print(f"Successfully loaded as JSON array with {len(data)} items")
                        if len(data) > 0:
                            print(f"First item keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'Not a dictionary'}")
                    else:
                        print("Successfully loaded as JSON object")
                        print(f"Top-level keys: {list(data.keys())}")
                    return
                except json.JSONDecodeError as e:
                    print(f"Error loading as regular JSON: {str(e)}")
            
            # Try as JSON lines
            f.seek(0)
            try:
                lines = []
                for i, line in enumerate(f):
                    if i >= 5:  # Just check first 5 lines
                        break
                    try:
                        json_obj = json.loads(line)
                        lines.append(json_obj)
                    except json.JSONDecodeError:
                        print(f"Line {i+1} is not valid JSON")
                
                if lines:
                    print(f"Format appears to be JSON lines (one JSON object per line)")
                    print(f"Successfully parsed {len(lines)} of first 5 lines")
                    if len(lines) > 0:
                        print(f"First item keys: {list(lines[0].keys()) if isinstance(lines[0], dict) else 'Not a dictionary'}")
                    return
            except Exception as e:
                print(f"Error checking as JSON lines: {str(e)}")
            
            # If we get here, try to count lines and show a sample
            f.seek(0)
            try:
                sample_lines = [next(f) for _ in range(3) if f]
                print(f"First few lines of the file:")
                for i, line in enumerate(sample_lines):
                    print(f"Line {i+1}: {line[:100]}...")
            except Exception as e:
                print(f"Error reading sample lines: {str(e)}")
                
    except Exception as e:
        print(f"Error opening file: {str(e)}")

def main():
    """Main function to search for and check JSON files."""
    search_paths = [
        "data/processed/*.json",
        "data/raw/*.json",
        "data/mapreduce/processed/*.json",
        "data/*/*.json"
    ]
    
    found_files = []
    
    # Search for files
    for path in search_paths:
        files = glob.glob(path)
        for file in files:
            if os.path.isfile(file) and file not in found_files:
                found_files.append(file)
    
    if not found_files:
        print("No JSON files found in the specified paths.")
        return
    
    print(f"Found {len(found_files)} JSON files:")
    for file in found_files:
        print(f"  {file}")
    
    # Check each file
    for file in found_files:
        check_file(file)
        
    print("\nFile check complete.")

if __name__ == "__main__":
    main()