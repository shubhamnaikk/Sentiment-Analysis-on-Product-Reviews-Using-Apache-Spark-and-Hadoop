#!/bin/bash

# Set up logging
LOG_FILE="logs/test_mapreduce.log"
mkdir -p logs

# Function for logging
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOG_FILE
}

log "Starting MapReduce test script..."

# Check if mapper and reducer are provided
if [ $# -lt 2 ]; then
    log "Usage: $0 <mapper_script> <reducer_script> [input_dir] [output_dir]"
    log "Example: $0 src/mapreduce/samples/sample_mapper.py src/mapreduce/samples/sample_reducer.py"
    exit 1
fi

MAPPER=$1
REDUCER=$2
INPUT_DIR=${3:-"data/mapreduce/input"}
OUTPUT_DIR=${4:-"/user/amazon_reviews/mapreduce/test_output"}

# Check if mapper and reducer exist
if [ ! -f "$MAPPER" ]; then
    log "Error: Mapper script not found: $MAPPER"
    exit 1
fi

if [ ! -f "$REDUCER" ]; then
    log "Error: Reducer script not found: $REDUCER"
    exit 1
fi

# Make scripts executable
chmod +x "$MAPPER"
chmod +x "$REDUCER"

log "Using mapper: $MAPPER"
log "Using reducer: $REDUCER"
log "Input directory: $INPUT_DIR"
log "Output directory: $OUTPUT_DIR"

# Find Hadoop streaming jar
HADOOP_STREAMING_JAR=$(find $HADOOP_HOME -name 'hadoop-streaming*.jar' | head -1)

if [ -z "$HADOOP_STREAMING_JAR" ]; then
    log "Error: Could not find Hadoop streaming jar"
    exit 1
fi

log "Using Hadoop streaming jar: $HADOOP_STREAMING_JAR"

# Clean output directory if it exists
hdfs dfs -rm -r -f $OUTPUT_DIR

# Run the Hadoop Streaming job
log "Submitting MapReduce job..."

hadoop jar $HADOOP_STREAMING_JAR \
  -files $MAPPER,$REDUCER \
  -mapper "python $(basename $MAPPER)" \
  -reducer "python $(basename $REDUCER)" \
  -input $INPUT_DIR \
  -output $OUTPUT_DIR

# Check if job was successful
if [ $? -eq 0 ]; then
    log "MapReduce job completed successfully"
    log "Sample output:"
    hdfs dfs -cat $OUTPUT_DIR/part-00000 | head -n 10
else
    log "Error: MapReduce job failed"
    exit 1
fi

log "MapReduce test completed"