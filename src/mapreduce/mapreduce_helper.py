import os
import subprocess
import logging
import json
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/mapreduce_helper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

def find_hadoop_streaming_jar():
    """Find the Hadoop streaming jar in the Hadoop installation"""
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        logger.error("HADOOP_HOME environment variable is not set")
        return None
    
    potential_paths = [
        f"{hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar",
        f"{hadoop_home}/share/hadoop/mapreduce/hadoop-streaming-*.jar"
    ]
    
    for path_pattern in potential_paths:
        matching_files = list(Path(os.path.dirname(path_pattern)).glob(os.path.basename(path_pattern)))
        if matching_files:
            return str(matching_files[0])
    
    logger.error("Could not find Hadoop Streaming jar")
    return None

def prepare_sample_data():
    """Prepare sample data for MapReduce development"""
    logger.info("Preparing sample data for MapReduce development...")
    
    # Create directories
    os.makedirs("data/mapreduce/input", exist_ok=True)
    os.makedirs("data/mapreduce/output", exist_ok=True)
    
    # Check if sample data already exists locally
    if os.path.exists("data/mapreduce/input/sample_data.json"):
        logger.info("Sample data already exists, skipping download")
        return True
    
    # Check if processed data exists in HDFS
    check_cmd = "hdfs dfs -ls /user/amazon_reviews/processed/all_reviews_processed"
    check_process = subprocess.run(check_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if check_process.returncode != 0:
        logger.error("Processed data not found in HDFS. Please run preprocessing first.")
        return False
    
    # Create a small sample in HDFS
    logger.info("Creating sample data in HDFS...")
    sample_cmd = """
    spark-submit --master local[*] --driver-memory 2g --class org.apache.spark.sql.execution.datasources.json.JsonSampleApp \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    hdfs:///user/amazon_reviews/processed/all_reviews_processed \
    hdfs:///user/amazon_reviews/mapreduce/sample_data \
    1000
    """
    
    try:
        # Create sample directory in HDFS
        subprocess.run("hdfs dfs -mkdir -p /user/amazon_reviews/mapreduce", shell=True, check=True)
        
        # Use PySpark to create a sample instead of the Java example
        sample_pyspark = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Create Sample").getOrCreate()
df = spark.read.parquet("/user/amazon_reviews/processed/all_reviews_processed")
sample = df.sample(False, 0.01, 42).limit(1000)
sample.write.json("/user/amazon_reviews/mapreduce/sample_data")
spark.stop()
        """
        
        # Write the sample script to a temporary file
        with open("temp_sample.py", "w") as f:
            f.write(sample_pyspark)
        
        # Run the sample script
        subprocess.run("spark-submit temp_sample.py", shell=True, check=True)
        
        # Remove the temporary file
        os.remove("temp_sample.py")
        
        # Get the sample data from HDFS
        logger.info("Fetching sample data from HDFS...")
        subprocess.run(
            "hdfs dfs -get /user/amazon_reviews/mapreduce/sample_data/* data/mapreduce/input/",
            shell=True,
            check=True
        )
        
        logger.info("Sample data prepared successfully")
        return True
    
    except Exception as e:
        logger.error(f"Error preparing sample data: {str(e)}")
        return False

def create_sample_scripts():
    """Create sample mapper and reducer scripts for testing"""
    logger.info("Creating sample MapReduce scripts...")
    
    # Create directories
    os.makedirs("src/mapreduce/samples", exist_ok=True)
    
    # Sample mapper
    mapper_path = "src/mapreduce/samples/sample_mapper.py"
    with open(mapper_path, "w") as f:
        f.write("""#!/usr/bin/env python3
import sys
import json

# Process each line from stdin
for line in sys.stdin:
    try:
        # Parse input line as JSON
        record = json.loads(line.strip())
        
        # Extract product_id and rating
        product_id = record.get('product_id')
        rating = record.get('rating')
        
        if product_id and rating:
            # Output as key-value pair for the reducer
            print(f"{product_id}\\t{rating}")
    except Exception as e:
        # Skip malformed lines
        continue
""")
    
    # Sample reducer
    reducer_path = "src/mapreduce/samples/sample_reducer.py"
    with open(reducer_path, "w") as f:
        f.write("""#!/usr/bin/env python3
import sys

current_product = None
rating_sum = 0
rating_count = 0

# Process each line from stdin (output from mappers)
for line in sys.stdin:
    try:
        # Parse input line
        product_id, rating = line.strip().split('\\t')
        rating = float(rating)
        
        # If this is a new product, output the previous one
        if current_product and current_product != product_id:
            avg_rating = rating_sum / rating_count
            print(f"{current_product}\\t{avg_rating:.2f}\\t{rating_count}")
            rating_sum = 0
            rating_count = 0
        
        # Update current product
        current_product = product_id
        rating_sum += rating
        rating_count += 1
    except Exception as e:
        # Skip malformed lines
        continue

# Output the last product
if current_product:
    avg_rating = rating_sum / rating_count
    print(f"{current_product}\\t{avg_rating:.2f}\\t{rating_count}")
""")
    
    # Make scripts executable
    os.chmod(mapper_path, 0o755)
    os.chmod(reducer_path, 0o755)
    
    logger.info(f"Created sample mapper: {mapper_path}")
    logger.info(f"Created sample reducer: {reducer_path}")
    
    return mapper_path, reducer_path

def run_mapreduce_job(mapper, reducer, input_path, output_path, hadoop_streaming_jar=None):
    """Run a MapReduce job using Hadoop Streaming"""
    logger.info(f"Running MapReduce job: {mapper} -> {reducer}")
    
    # Find Hadoop Streaming jar if not provided
    if not hadoop_streaming_jar:
        hadoop_streaming_jar = find_hadoop_streaming_jar()
        if not hadoop_streaming_jar:
            return False
    
    # Ensure mapper and reducer are executable
    os.chmod(mapper, 0o755)
    os.chmod(reducer, 0o755)
    
    # Clean output directory if it exists in HDFS
    clean_cmd = f"hdfs dfs -rm -r -f {output_path}"
    subprocess.run(clean_cmd, shell=True)
    
    # Run the Hadoop Streaming job
    cmd = f"""
    hadoop jar {hadoop_streaming_jar} \
      -files {mapper},{reducer} \
      -mapper "python {os.path.basename(mapper)}" \
      -reducer "python {os.path.basename(reducer)}" \
      -input {input_path} \
      -output {output_path}
    """
    
    logger.info("Submitting Hadoop Streaming job...")
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if process.returncode == 0:
        logger.info("MapReduce job completed successfully")
        
        # Get job output
        result_cmd = f"hdfs dfs -cat {output_path}/part-00000 | head -n 10"
        result = subprocess.run(result_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        if result.returncode == 0:
            logger.info("Sample output:")
            output = result.stdout.decode()
            logger.info(output)
            return True
        else:
            logger.error(f"Error reading job output: {result.stderr.decode()}")
            return False
    else:
        logger.error(f"MapReduce job failed: {process.stderr.decode()}")
        return False

def main():
    """Main function to set up MapReduce development environment"""
    logger.info("Setting up MapReduce development environment...")
    
    # Find Hadoop streaming jar
    hadoop_streaming_jar = find_hadoop_streaming_jar()
    if not hadoop_streaming_jar:
        logger.error("Cannot continue without Hadoop Streaming jar")
        return
    
    logger.info(f"Found Hadoop Streaming jar: {hadoop_streaming_jar}")
    
    # Prepare sample data
    if not prepare_sample_data():
        logger.error("Failed to prepare sample data, exiting")
        return
    
    # Create sample scripts
    mapper_path, reducer_path = create_sample_scripts()
    
    # Run sample job
    sample_input = "data/mapreduce/input"
    sample_output = "/user/amazon_reviews/mapreduce/sample_output"
    
    logger.info("Testing MapReduce environment with sample job...")
    if run_mapreduce_job(mapper_path, reducer_path, sample_input, sample_output, hadoop_streaming_jar):
        logger.info("MapReduce environment is ready for development")
    else:
        logger.error("Failed to run sample MapReduce job")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")