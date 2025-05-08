#!/bin/bash

# Set up logging
LOG_FILE="logs/hadoop_services.log"
mkdir -p logs

# Function for logging
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOG_FILE
}

log "Starting Hadoop and related services..."

# Check if HADOOP_HOME is set
if [ -z "$HADOOP_HOME" ]; then
    log "Error: HADOOP_HOME environment variable is not set"
    log "Please set HADOOP_HOME to your Hadoop installation directory"
    exit 1
fi

# Start HDFS services
log "Starting HDFS services..."
$HADOOP_HOME/sbin/start-dfs.sh

# Check if NameNode is running
jps | grep -q "NameNode"
if [ $? -ne 0 ]; then
    log "Error: NameNode did not start properly"
    exit 1
else
    log "NameNode started successfully"
fi

# Start YARN services
log "Starting YARN services..."
$HADOOP_HOME/sbin/start-yarn.sh

# Check if ResourceManager is running
jps | grep -q "ResourceManager"
if [ $? -ne 0 ]; then
    log "Error: ResourceManager did not start properly"
    exit 1
else
    log "ResourceManager started successfully"
fi

# Create necessary HDFS directories if they don't exist
log "Creating HDFS directories..."
hdfs dfs -mkdir -p /user/amazon_reviews/raw
hdfs dfs -mkdir -p /user/amazon_reviews/processed
hdfs dfs -mkdir -p /user/amazon_reviews/mapreduce
hdfs dfs -mkdir -p /user/amazon_reviews/results
hdfs dfs -mkdir -p /spark-logs

# Set proper permissions
hdfs dfs -chmod -R 777 /user
hdfs dfs -chmod -R 777 /spark-logs

log "HDFS directories created and permissions set"

# If Spark is available, start Spark History Server
if [ ! -z "$SPARK_HOME" ]; then
    log "Starting Spark History Server..."
    $SPARK_HOME/sbin/start-history-server.sh
    
    if [ $? -eq 0 ]; then
        log "Spark History Server started successfully"
    else
        log "Warning: Failed to start Spark History Server"
    fi
else
    log "SPARK_HOME not set, skipping Spark History Server startup"
fi

log "All services started successfully"