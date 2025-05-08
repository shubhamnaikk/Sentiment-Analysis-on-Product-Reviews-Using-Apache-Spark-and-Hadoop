#!/bin/bash

# Set up logging
LOG_FILE="logs/hadoop_setup.log"
mkdir -p logs

# Function for logging
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOG_FILE
}

log "Starting Hadoop configuration..."

# Check if HADOOP_HOME is set
if [ -z "$HADOOP_HOME" ]; then
    log "Error: HADOOP_HOME environment variable is not set"
    log "Please set HADOOP_HOME to your Hadoop installation directory"
    exit 1
fi

log "HADOOP_HOME is set to $HADOOP_HOME"

# Create data directories for HDFS
log "Creating local directories for HDFS..."
mkdir -p ~/hadoop_data/namenode
mkdir -p ~/hadoop_data/datanode

# Copy configuration files to Hadoop
log "Copying configuration files to Hadoop..."
cp conf/hadoop/core-site.xml $HADOOP_HOME/etc/hadoop/
cp conf/hadoop/hdfs-site.xml $HADOOP_HOME/etc/hadoop/
cp conf/hadoop/mapred-site.xml $HADOOP_HOME/etc/hadoop/
cp conf/hadoop/yarn-site.xml $HADOOP_HOME/etc/hadoop/

log "Hadoop configuration completed"

# Format namenode if it doesn't exist
if [ ! -d ~/hadoop_data/namenode/current ]; then
    log "Formatting HDFS namenode..."
    $HADOOP_HOME/bin/hdfs namenode -format
    
    if [ $? -eq 0 ]; then
        log "Namenode formatted successfully"
    else
        log "Error formatting namenode"
        exit 1
    fi
else
    log "Namenode already formatted, skipping format step"
fi

log "Hadoop setup completed successfully"