def convert_parquet_to_json(category):
    """Convert parquet files to JSON format"""
    try:
        import pyarrow.parquet as pq
        import pandas as pd
        import numpy as np
        
        logger.info(f"Converting Parquet files to JSON for {category}")
        
        category_dir = f"data/raw/{category}"
        output_file = f"data/raw/{category}.json"
        
        # Check if directory exists
        if not os.path.exists(category_dir):
            logger.error(f"Directory not found: {category_dir}")
            return None
        
        # Find all parquet files
        parquet_files = sorted(Path(category_dir).glob("*.parquet"))
        
        if not parquet_files:
            logger.error(f"No parquet files found in {category_dir}")
            return None
        
        logger.info(f"Found {len(parquet_files)} parquet files to convert")
        
        # Custom JSON encoder to handle NumPy types
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
        
        # Try to import tqdm for progress bar
        try:
            from tqdm import tqdm
            parquet_files_iter = tqdm(parquet_files, desc=f"Converting {category}")
        except ImportError:
            parquet_files_iter = parquet_files
        
        # Process each file and append to output
        row_count = 0
        with open(output_file, 'w', encoding='utf-8') as out_f:
            for idx, parquet_file in enumerate(parquet_files_iter):
                try:
                    logger.info(f"Processing file {idx+1}/{len(parquet_files)}: {parquet_file}")
                    
                    # Read the parquet file
                    table = pq.read_table(parquet_file)
                    df = table.to_pandas()
                    
                    # Convert each row to JSON and write
                    for _, row in df.iterrows():
                        try:
                            # Convert row to dict
                            row_dict = row.to_dict()
                            
                            # Convert to JSON using custom encoder and write to file
                            json_line = json.dumps(row_dict, cls=NumpyEncoder)
                            out_f.write(json_line + '\n')
                            row_count += 1
                        except Exception as e:
                            logger.error(f"Error processing row: {str(e)}")
                            if row_count < 5:  # Only log details for the first few errors
                                logger.error(traceback.format_exc())
                        
                except Exception as e:
                    logger.error(f"Error processing {parquet_file}: {str(e)}")
                    logger.error(traceback.format_exc())
        
        logger.info(f"Converted {row_count} rows to {output_file}")
        
        # Compress the output file
        logger.info(f"Compressing {output_file} to {output_file}.gz")
        with open(output_file, 'rb') as f_in:
            with gzip.open(f"{output_file}.gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove the uncompressed file to save space
        os.remove(output_file)
        
        logger.info(f"Created compressed JSON file: {output_file}.gz")
        return f"{output_file}.gz"
    
    except ImportError as e:
        logger.error(f"Missing required package: {str(e)}")
        logger.error("Please install with: pip install pyarrow pandas")
        return None
    except Exception as e:
        logger.error(f"Error converting parquet files: {str(e)}")
        logger.error(traceback.format_exc())
        return None