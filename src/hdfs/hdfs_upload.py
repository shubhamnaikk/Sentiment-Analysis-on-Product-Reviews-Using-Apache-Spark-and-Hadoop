import os
import subprocess
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/hdfs_upload.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def ensure_hdfs_directories():
    """Ensure the HDFS directories exist"""
    logger.info("Creating HDFS directories...")
    
    directories = [
        "/user/amazon_reviews/raw",
        "/user/amazon_reviews/processed",
        "/user/amazon_reviews/mapreduce",
        "/user/amazon_reviews/results"
    ]
    
    for directory in directories:
        cmd = f"hdfs dfs -mkdir -p {directory}"
        logger.info(f"Creating directory: {directory}")
        
        process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if process.returncode != 0:
            logger.error(f"Error creating directory {directory}: {process.stderr.decode()}")
        else:
            logger.info(f"Directory {directory} created or already exists")

def upload_to_hdfs(local_path, hdfs_path):
    """Upload local file to HDFS"""
    # Check if file exists
    if not os.path.exists(local_path):
        logger.error(f"Local file does not exist: {local_path}")
        return False
    
    file_size = Path(local_path).stat().st_size / (1024 * 1024)  # Size in MB
    logger.info(f"Uploading {local_path} ({file_size:.2f} MB) to HDFS at {hdfs_path}")
    
    cmd = f"hdfs dfs -put -f {local_path} {hdfs_path}"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if process.returncode == 0:
        logger.info(f"Successfully uploaded {local_path} to HDFS")
        return True
    else:
        logger.error(f"Error uploading {local_path}: {process.stderr.decode()}")
        return False

def upload_all_data():
    """Upload all raw data files to HDFS"""
    # First ensure HDFS directories exist
    ensure_hdfs_directories()
    
    # Upload raw data
    raw_dir = "data/raw"
    hdfs_raw_dir = "/user/amazon_reviews/raw"
    
    uploaded_count = 0
    failed_count = 0
    
    for filename in os.listdir(raw_dir):
        if filename.endswith('.json.gz'):
            local_path = os.path.join(raw_dir, filename)
            hdfs_path = f"{hdfs_raw_dir}/{filename}"
            
            if upload_to_hdfs(local_path, hdfs_path):
                uploaded_count += 1
            else:
                failed_count += 1
    
    logger.info(f"Upload summary: {uploaded_count} files uploaded, {failed_count} files failed")

if __name__ == "__main__":
    logger.info("Starting HDFS upload process...")
    upload_all_data()
    logger.info("HDFS upload process completed")