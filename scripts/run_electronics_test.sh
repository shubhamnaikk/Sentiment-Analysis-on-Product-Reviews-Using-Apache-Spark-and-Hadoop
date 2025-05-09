#!/bin/bash

echo "Running MapReduce test for Electronics category..."

# Configuration
HADOOP_STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming*.jar" | head -1)
INPUT_PATH="/user/amazon_reviews/mapreduce/electronics_product_ratings"
SAMPLE_PATH="/user/amazon_reviews/mapreduce/electronics_sample"
AVG_OUTPUT_PATH="/user/amazon_reviews/results/electronics_average_ratings"
TOP_OUTPUT_PATH="/user/amazon_reviews/results/electronics_top_analysis"
MAPPER="../src/mapreduce/mapper.py"
REDUCER="../src/mapreduce/reducer.py"
TOP_MAPPER="../src/mapreduce/top_mapper.py"
TOP_REDUCER="../src/mapreduce/top_reducer.py"
COMBINER="../src/mapreduce/combiner.py"

# Make sure scripts are executable
chmod +x $MAPPER $REDUCER $TOP_MAPPER $TOP_REDUCER $COMBINER

# Create a sample from electronics data (first 1000 lines)
echo "Creating a sample from Electronics data..."
hadoop fs -mkdir -p /user/amazon_reviews/results
hadoop fs -rm -r -f $SAMPLE_PATH
hadoop fs -cat $INPUT_PATH | head -n 1000 | hadoop fs -put - $SAMPLE_PATH

# Test if we have data
LINES=$(hadoop fs -cat $SAMPLE_PATH | wc -l)
echo "Sample contains $LINES lines"

# Run local test first to verify logic
echo "Running local test with sample data..."
mkdir -p test_results
hadoop fs -cat $SAMPLE_PATH | head -n 100 > test_results/local_input.txt
cat test_results/local_input.txt | python3 $MAPPER | sort | python3 $REDUCER > test_results/local_avg.txt
cat test_results/local_avg.txt | python3 $TOP_MAPPER | sort | python3 $TOP_REDUCER > test_results/local_top.txt

echo "Local test results:"
cat test_results/local_top.txt

# Step 1: Run average ratings job
echo "Running average ratings job on HDFS data..."
hadoop fs -rm -r -f $AVG_OUTPUT_PATH

hadoop jar $HADOOP_STREAMING_JAR \
    -D mapred.job.name="Electronics - Average Ratings (Test)" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.map.memory.mb=1024 \
    -D mapreduce.reduce.memory.mb=1024 \
    -files $MAPPER,$REDUCER,$COMBINER \
    -mapper "python3 $(basename $MAPPER)" \
    -reducer "python3 $(basename $REDUCER)" \
    -combiner "python3 $(basename $COMBINER)" \
    -input $SAMPLE_PATH \
    -output $AVG_OUTPUT_PATH

# Check if the job completed successfully
if [ $? -ne 0 ]; then
    echo "First job failed. Falling back to local testing."
    mkdir -p results
    cp test_results/local_top.txt results/electronics_top_analysis_results.txt
    echo "Results saved to results/electronics_top_analysis_results.txt"
    exit 1
fi

# Show intermediate results
echo "Average ratings results:"
hadoop fs -cat "$AVG_OUTPUT_PATH/part-*" | head -n 10

# Step 2: Run top analysis job
echo "Running top analysis job..."
hadoop fs -rm -r -f $TOP_OUTPUT_PATH

hadoop jar $HADOOP_STREAMING_JAR \
    -D mapred.job.name="Electronics - Top Analysis (Test)" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.map.memory.mb=1024 \
    -D mapreduce.reduce.memory.mb=1024 \
    -files $TOP_MAPPER,$TOP_REDUCER \
    -mapper "python3 $(basename $TOP_MAPPER)" \
    -reducer "python3 $(basename $TOP_REDUCER)" \
    -input $AVG_OUTPUT_PATH \
    -output $TOP_OUTPUT_PATH

# Check if the job completed successfully
if [ $? -ne 0 ]; then
    echo "Second job failed. Falling back to local testing."
    mkdir -p results
    cp test_results/local_top.txt results/electronics_top_analysis_results.txt
    echo "Results saved to results/electronics_top_analysis_results.txt"
    exit 1
fi

# Save results
mkdir -p results
hadoop fs -cat "$TOP_OUTPUT_PATH/part-*" > results/electronics_top_analysis_results.txt

echo "MapReduce pipeline completed successfully!"
echo "Results saved to results/electronics_top_analysis_results.txt:"
cat results/electronics_top_analysis_results.txt
