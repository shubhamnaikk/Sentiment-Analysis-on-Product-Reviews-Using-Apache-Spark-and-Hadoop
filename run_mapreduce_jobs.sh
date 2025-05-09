#!/bin/bash

# Configuration
HADOOP_STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming*.jar" | head -1)
BASE_INPUT_DIR="/user/amazon_reviews/mapreduce"
BASE_OUTPUT_DIR="/user/amazon_reviews/analysis_results"
CATEGORIES=("electronics" "toys_games" "cell_phones" "all_reviews")

# Paths to mapper and reducer scripts
MAPPER="src/mapreduce/mapper.py"
REDUCER="src/mapreduce/reducer.py"
TOP_MAPPER="src/mapreduce/top_mapper.py"
TOP_REDUCER="src/mapreduce/top_reducer.py"
COMBINER="src/mapreduce/combiner.py"


# Make sure scripts are executable
chmod +x $MAPPER $REDUCER $TOP_MAPPER $TOP_REDUCER $COMBINER

# Function to run a MapReduce job
run_job() {
    local input_path=$1
    local output_path=$2
    local mapper=$3
    local reducer=$4
    local combiner=$5
    local job_name=$6
    
    echo "Running $job_name..."
    
    # Remove output directory if it exists
    hadoop fs -rm -r -f $output_path
    
    # Run the MapReduce job
    hadoop jar $HADOOP_STREAMING_JAR \
        -D mapred.job.name="$job_name" \
        -D mapreduce.job.reduces=5 \
        -D mapreduce.map.memory.mb=2048 \
        -D mapreduce.reduce.memory.mb=2048 \
        -files $mapper,$reducer \
        -mapper "python3 $(basename $mapper)" \
        -reducer "python3 $(basename $reducer)" \
        -input $input_path \
        -output $output_path
    
    echo "$job_name completed"
    echo "Results in: $output_path"
    echo
}

# Process each category
for category in "${CATEGORIES[@]}"; do
    echo "Processing $category category..."
    
    # Step 1: Calculate average ratings with basic mapper/reducer
    rating_input="$BASE_INPUT_DIR/${category}_product_ratings"
    rating_output="$BASE_OUTPUT_DIR/${category}_average_ratings"
    
    run_job "$rating_input" "$rating_output" "$MAPPER" "$REDUCER" "$COMBINER" "$category - Average Ratings"
    
    # Step 2: Find top products/categories with specialized mapper/reducer
    top_input="$rating_output"
    top_output="$BASE_OUTPUT_DIR/${category}_top_analysis"
    
    run_job "$top_input" "$top_output" "$TOP_MAPPER" "$TOP_REDUCER" "$category - Top Analysis"
    
    # Copy results to a local file for easy viewing
    echo "Copying results to local file..."
    hadoop fs -cat "$top_output/part-*" > "${category}_top_analysis_results.txt"
    
    echo "Completed analysis for $category"
    echo "================================================"
    echo
done

echo "All MapReduce jobs completed successfully!"
echo "Check the results in:"
echo "$BASE_OUTPUT_DIR"
echo "and in the local text files: *_top_analysis_results.txt"