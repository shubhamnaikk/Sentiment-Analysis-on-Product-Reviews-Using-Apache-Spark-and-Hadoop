import subprocess
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/hdfs_utils.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

def check_hdfs_path_exists(hdfs_path):
    """Check if a path exists in HDFS"""
    cmd = f"hdfs dfs -test -e {hdfs_path}"
    process = subprocess.run(cmd, shell=True)
    return process.returncode == 0

def get_hdfs_file_size(hdfs_path):
    """Get the size of a file or directory in HDFS"""
    if not check_hdfs_path_exists(hdfs_path):
        logger.error(f"Path does not exist in HDFS: {hdfs_path}")
        return None
    
    cmd = f"hdfs dfs -du -s -h {hdfs_path}"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if process.returncode == 0:
        output = process.stdout.decode().strip()
        parts = output.split()
        if len(parts) >= 2:
            return parts[0]  # Return the size part
    
    logger.error(f"Error getting size for {hdfs_path}: {process.stderr.decode()}")
    return None

def list_hdfs_directory(hdfs_path):
    """List files in an HDFS directory"""
    if not check_hdfs_path_exists(hdfs_path):
        logger.error(f"Path does not exist in HDFS: {hdfs_path}")
        return []
    
    cmd = f"hdfs dfs -ls {hdfs_path}"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if process.returncode == 0:
        output = process.stdout.decode().strip()
        lines = output.split('\n')
        files = []
        for line in lines[1:]:  # Skip the first line (total count)
            parts = line.split()
            if len(parts) >= 8:
                file_info = {
                    'permissions': parts[0],
                    'owner': parts[2],
                    'group': parts[3],
                    'size': parts[4],
                    'date': f"{parts[5]} {parts[6]}",
                    'path': parts[7]
                }
                files.append(file_info)
        return files
    
    logger.error(f"Error listing directory {hdfs_path}: {process.stderr.decode()}")
    return []

def download_from_hdfs(hdfs_path, local_path):
    """Download a file from HDFS to local filesystem"""
    if not check_hdfs_path_exists(hdfs_path):
        logger.error(f"Path does not exist in HDFS: {hdfs_path}")
        return False
    
    # Create the local directory if it doesn't exist
    local_dir = os.path.dirname(local_path)
    if local_dir and not os.path.exists(local_dir):
        os.makedirs(local_dir)
    
    cmd = f"hdfs dfs -get {hdfs_path} {local_path}"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if process.returncode == 0:
        logger.info(f"Successfully downloaded {hdfs_path} to {local_path}")
        return True
    else:
        logger.error(f"Error downloading {hdfs_path}: {process.stderr.decode()}")
        return False

def delete_hdfs_path(hdfs_path):
    """Delete a file or directory in HDFS"""
    if not check_hdfs_path_exists(hdfs_path):
        logger.warning(f"Path does not exist in HDFS: {hdfs_path}")
        return True  # Already deleted
    
    cmd = f"hdfs dfs -rm -r {hdfs_path}"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if process.returncode == 0:
        logger.info(f"Successfully deleted {hdfs_path}")
        return True
    else:
        logger.error(f"Error deleting {hdfs_path}: {process.stderr.decode()}")
        return False