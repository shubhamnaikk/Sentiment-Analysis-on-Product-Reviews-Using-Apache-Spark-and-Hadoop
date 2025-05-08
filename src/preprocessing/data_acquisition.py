
import requests
import os
import gzip
import shutil
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/data_acquisition.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Create directories
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Updated URLs for the dataset from Amazon-Reviews-2023 GitHub
urls = {
    "Electronics": "https://github.com/amazon-reviews-2023/amazon-reviews-2023.github.io/raw/main/datasets/Electronics.json.gz",
    "Toys_Games": "https://github.com/amazon-reviews-2023/amazon-reviews-2023.github.io/raw/main/datasets/Toys_and_Games.json.gz", 
    "Cell_Phones": "https://github.com/amazon-reviews-2023/amazon-reviews-2023.github.io/raw/main/datasets/Cell_Phones_and_Accessories.json.gz"
}

def download_file(url, category):
    """Download file from URL to the raw data directory"""
    local_filename = f"data/raw/{category}.json.gz"
    logger.info(f"Downloading {category} dataset from {url}...")
    
    try:
        # Stream the file to avoid loading entire file into memory
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        file_size = Path(local_filename).stat().st_size / (1024 * 1024)  # Size in MB
        logger.info(f"Downloaded {category} dataset to {local_filename} ({file_size:.2f} MB)")
        return local_filename
    except Exception as e:
        logger.error(f"Error downloading {category}: {str(e)}")
        return None

def extract_sample(category, sample_size=1000):
    """Extract a sample of the dataset for quick testing"""
    input_file = f"data/raw/{category}.json.gz"
    output_file = f"data/raw/{category}_sample.json"
    
    if not os.path.exists(input_file):
        logger.error(f"Cannot extract sample: Input file {input_file} does not exist")
        return None
    
    logger.info(f"Extracting sample of {sample_size} records from {category} dataset...")
    
    count = 0
    with gzip.open(input_file, 'rt', encoding='utf-8') as f_in:
        with open(output_file, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                f_out.write(line)
                count += 1
                if count >= sample_size:
                    break
    
    logger.info(f"Extracted {count} records to {output_file}")
    return output_file

def download_all_datasets():
    """Download all datasets and create samples"""
    for category, url in urls.items():
        try:
            local_file = download_file(url, category)
            if local_file:
                extract_sample(category)
        except Exception as e:
            logger.error(f"Error processing {category}: {str(e)}")

if __name__ == "__main__":
    logger.info("Starting data acquisition process...")
    download_all_datasets()
    logger.info("Data acquisition process completed")