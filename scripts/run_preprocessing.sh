#!/bin/bash

# Set up logging
LOG_FILE="logs/preprocessing_job.log"
mkdir -p logs

# Function for logging
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOG_FILE
}

log "Starting preprocessing job..."

# Check if SPARK_HOME is set
if [ -z "$SPARK_HOME" ]; then
    log "Warning: SPARK_HOME environment variable is not set"
    log "Using spark-submit from PATH"
    SPARK_SUBMIT="spark-submit"
else
    log "SPARK_HOME is set to $SPARK_HOME"
    SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
fi

# Verify Hadoop is running
hdfs dfs -ls / > /dev/null 2>&1
if [ $? -ne 0 ]; then
    log "Error: HDFS is not running or not accessible"
    log "Please start Hadoop before running this script"
    exit 1
fi

log "Submitting Spark preprocessing job..."

# Run the preprocessing job with Spark
$SPARK_SUBMIT \
  --master local[*] \
  --driver-memory 4g \
  --executor-memory 4g \
  src/preprocessing/data_preprocessing.py

# Check if job was successful
if [ $? -eq 0 ]; then
    log "Preprocessing job completed successfully"
else
    log "Error: Preprocessing job failed"
    exit 1
fi

# Verify data was written to HDFS
hdfs dfs -ls /user/amazon_reviews/processed/all_reviews_processed > /dev/null 2>&1
if [ $? -eq 0 ]; then
    log "Verified processed data exists in HDFS"
else
    log "Warning: Could not verify processed data in HDFS"
fi

log "Preprocessing pipeline completed"