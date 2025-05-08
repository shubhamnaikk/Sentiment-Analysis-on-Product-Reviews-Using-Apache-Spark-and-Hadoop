from pyspark.sql import SparkSession
import logging
import os
import subprocess

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/format_converter.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

def create_spark_session():
    """Create and return a Spark session"""
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName("Amazon Reviews Format Converter") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    logger.info(f"Created Spark session: {spark.version}")
    return spark

def convert_parquet_to_json(spark, input_path, output_path):
    """Convert Parquet files to JSON for easier MapReduce processing"""
    logger.info(f"Converting Parquet files from {input_path} to JSON")
    
    try:
        # Load Parquet data
        df = spark.read.parquet(input_path)
        record_count = df.count()
        logger.info(f"Loaded {record_count} records from Parquet")
        
        # Save as JSON (one file per partition)
        df.write.json(output_path, mode="overwrite")
        logger.info(f"Converted to JSON format at {output_path}")
        
        return True
    except Exception as e:
        logger.error(f"Error converting to JSON: {str(e)}")
        return False

def convert_parquet_to_csv(spark, input_path, output_path):
    """Convert Parquet files to CSV for easier MapReduce processing"""
    logger.info(f"Converting Parquet files from {input_path} to CSV")
    
    try:
        # Load Parquet data
        df = spark.read.parquet(input_path)
        record_count = df.count()
        logger.info(f"Loaded {record_count} records from Parquet")
        
        # Save as CSV with header
        df.write.option("header", "true").csv(output_path, mode="overwrite")
        logger.info(f"Converted to CSV format at {output_path}")
        
        return True
    except Exception as e:
        logger.error(f"Error converting to CSV: {str(e)}")
        return False

def prepare_mapreduce_formats():
    """Prepare data in formats suitable for MapReduce"""
    spark = create_spark_session()
    
    categories = ["electronics", "toys_games", "cell_phones", "all_reviews"]
    formats = [
        {"func": convert_parquet_to_json, "name": "json"},
        {"func": convert_parquet_to_csv, "name": "csv"}
    ]
    
    # Create HDFS directories
    subprocess.run("hdfs dfs -mkdir -p /user/amazon_reviews/mapreduce", shell=True)
    
    # Create local directories
    os.makedirs("data/mapreduce", exist_ok=True)
    
    for category in categories:
        input_path = f"/user/amazon_reviews/processed/{category}_processed"
        
        # Check if input exists
        check_cmd = f"hdfs dfs -ls {input_path}"
        check_process = subprocess.run(check_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        if check_process.returncode != 0:
            logger.warning(f"Input path does not exist: {input_path}, skipping category")
            continue
        
        for format_info in formats:
            format_name = format_info["name"]
            converter_func = format_info["func"]
            
            output_path = f"/user/amazon_reviews/mapreduce/{category}_{format_name}"
            
            logger.info(f"Processing {category} to {format_name}...")
            success = converter_func(spark, input_path, output_path)
            
            if success:
                # Download a sample to local filesystem
                local_dir = f"data/mapreduce/{category}_{format_name}"
                os.makedirs(local_dir, exist_ok=True)
                
                sample_cmd = f"hdfs dfs -get {output_path}/part-* {local_dir}/ 2>/dev/null | head -n 100"
                subprocess.run(sample_cmd, shell=True)
                
                logger.info(f"Downloaded sample of {category}_{format_name} to {local_dir}")
    
    spark.stop()
    logger.info("Format conversion completed")

if __name__ == "__main__":
    try:
        prepare_mapreduce_formats()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")