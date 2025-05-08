import argparse
import subprocess
import os
import logging
import time
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/test_mapreduce_job.log"),
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

def run_mapreduce_job(mapper, reducer, input_path, output_path, hadoop_streaming_jar):
    """Run a MapReduce job and measure performance"""
    logger.info(f"Running MapReduce job: {mapper} -> {reducer}")
    
    # Ensure the mapper and reducer are executable
    os.chmod(mapper, 0o755)
    os.chmod(reducer, 0o755)
    
    # Remove output directory if it exists
    subprocess.run(f"hdfs dfs -rm -r -f {output_path}", shell=True)
    
    # Build the Hadoop Streaming command
    cmd = f"""
    hadoop jar {hadoop_streaming_jar} \
      -files {mapper},{reducer} \
      -mapper "python {os.path.basename(mapper)}" \
      -reducer "python {os.path.basename(reducer)}" \
      -input {input_path} \
      -output {output_path} \
      -numReduceTasks 5
    """
    
    # Start timing
    start_time = time.time()
    
    # Run the job
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # End timing
    end_time = time.time()
    duration = end_time - start_time
    
    # Check if the job was successful
    if process.returncode == 0:
        logger.info(f"MapReduce job completed successfully in {duration:.2f} seconds")
        
        # Get job statistics
        job_stats = {
            "duration_seconds": duration,
            "status": "success",
            "mapper": mapper,
            "reducer": reducer,
            "input_path": input_path,
            "output_path": output_path,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Save statistics
        os.makedirs("mapreduce/stats", exist_ok=True)
        stats_file = f"mapreduce/stats/{os.path.basename(mapper).replace('.py', '')}_{int(time.time())}.json"
        
        with open(stats_file, "w") as f:
            json.dump(job_stats, f, indent=2)
        
        # Check output
        output_sample_cmd = f"hdfs dfs -cat {output_path}/part-* | head -n 10"
        output_sample = subprocess.run(output_sample_cmd, shell=True, stdout=subprocess.PIPE)
        
        logger.info("Sample output:")
        logger.info(output_sample.stdout.decode())
        
        return True
    else:
        logger.error("MapReduce job failed")
        logger.error(process.stderr.decode())
        return False

def main():
    parser = argparse.ArgumentParser(description="Run a MapReduce job and measure performance")
    parser.add_argument("mapper", help="Path to mapper script")
    parser.add_argument("reducer", help="Path to reducer script")
    parser.add_argument("--input", required=True, help="HDFS input path")
    parser.add_argument("--output", required=True, help="HDFS output path")
    parser.add_argument("--jar", help="Path to Hadoop Streaming jar")
    
    args = parser.parse_args()
    
    # Find Hadoop Streaming jar if not provided
    hadoop_streaming_jar = args.jar
    if not hadoop_streaming_jar:
        hadoop_home = os.environ.get("HADOOP_HOME", "/usr/local/hadoop")
        jar_cmd = f"find {hadoop_home} -name 'hadoop-streaming*.jar' | head -1"
        hadoop_streaming_jar = subprocess.run(jar_cmd, shell=True, stdout=subprocess.PIPE).stdout.decode().strip()
    
    if not hadoop_streaming_jar:
        logger.error("Could not find Hadoop Streaming jar. Please provide it with --jar.")
        return
    
    logger.info(f"Using Hadoop Streaming jar: {hadoop_streaming_jar}")
    
    run_mapreduce_job(args.mapper, args.reducer, args.input, args.output, hadoop_streaming_jar)

if __name__ == "__main__":
    main()