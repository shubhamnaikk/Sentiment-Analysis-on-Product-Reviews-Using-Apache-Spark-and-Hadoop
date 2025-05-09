#!/bin/bash
# Script to run all Spark analysis jobs

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Navigate to the project root directory
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT" || exit 1

# Source environment variables (if needed)
if [ -f "$PROJECT_ROOT/.env" ]; then
    source "$PROJECT_ROOT/.env"
fi

# Default variables
INPUT_PATH="hdfs:///user/hadoop/amazon_reviews"
OUTPUT_PATH="hdfs:///user/hadoop/results"
FORMAT="parquet"
LOCAL_OUTPUT_DIR="$PROJECT_ROOT/results"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --input)
        INPUT_PATH="$2"
        shift
        shift
        ;;
        --output)
        OUTPUT_PATH="$2"
        shift
        shift
        ;;
        --format)
        FORMAT="$2"
        shift
        shift
        ;;
        --local-output)
        LOCAL_OUTPUT_DIR="$2"
        shift
        shift
        ;;
        --help)
        echo "Usage: $0 [options]"
        echo "Options:"
        echo "  --input PATH       Input data path (default: $INPUT_PATH)"
        echo "  --output PATH      HDFS output directory (default: $OUTPUT_PATH)"
        echo "  --format FORMAT    Input data format (default: $FORMAT)"
        echo "  --local-output DIR Local directory to save results (default: $LOCAL_OUTPUT_DIR)"
        echo "  --help             Show this help message"
        exit 0
        ;;
        *)
        echo "Unknown option: $1"
        exit 1
        ;;
    esac
done

# Create the local output directory if it doesn't exist
mkdir -p "$LOCAL_OUTPUT_DIR"

# Create HDFS output directory if it doesn't exist
hadoop fs -test -d "$OUTPUT_PATH" || hadoop fs -mkdir -p "$OUTPUT_PATH"

# Setup environment (activate virtual environment if exists)
if [ -d "$PROJECT_ROOT/venv_py310" ]; then
    echo "Activating Python virtual environment..."
    source "$PROJECT_ROOT/venv_py310/bin/activate"
fi

# Create subdirectories for each analysis type
for subdir in "trends" "bias_detection" "statistics" "optimization"; do
    hadoop fs -test -d "$OUTPUT_PATH/$subdir" || hadoop fs -mkdir -p "$OUTPUT_PATH/$subdir"
    mkdir -p "$LOCAL_OUTPUT_DIR/$subdir"
done

echo "Starting Amazon Reviews Spark Analysis..."
echo "Input path: $INPUT_PATH"
echo "Output path: $OUTPUT_PATH"
echo "Format: $FORMAT"

# Run the ratings trend analysis
echo "Running Ratings Trend Analysis..."
spark-submit \
    --master local[*] \
    --name "Amazon Reviews Ratings Trend Analysis" \
    src/spark_analysis/ratings_trend_analysis.py \
    --input "$INPUT_PATH" \
    --output "$OUTPUT_PATH/trends" \
    --format "$FORMAT"

if [ $? -ne 0 ]; then
    echo "Error: Ratings trend analysis failed."
    exit 1
fi

# Run the review bias detection
echo "Running Review Bias Detection..."
spark-submit \
    --master local[*] \
    --name "Amazon Reviews Bias Detection" \
    src/spark_analysis/review_bias_detection.py \
    --input "$INPUT_PATH" \
    --output "$OUTPUT_PATH/bias_detection" \
    --format "$FORMAT"

if [ $? -ne 0 ]; then
    echo "Error: Review bias detection failed."
    exit 1
fi

# Run the ratings statistics analysis
echo "Running Ratings Statistics Analysis..."
spark-submit \
    --master local[*] \
    --name "Amazon Reviews Ratings Statistics" \
    src/spark_analysis/ratings_statistics.py \
    --input "$INPUT_PATH" \
    --output "$OUTPUT_PATH/statistics" \
    --format "$FORMAT"

if [ $? -ne 0 ]; then
    echo "Error: Ratings statistics analysis failed."
    exit 1
fi

# Run the Spark optimization benchmarks (optional, can be resource-intensive)
echo "Running Spark Optimization Benchmarks..."
spark-submit \
    --master local[*] \
    --name "Amazon Reviews Spark Optimization" \
    --conf spark.executor.memory=4g \
    --conf spark.driver.memory=4g \
    src/spark_analysis/spark_optimization.py \
    --input "$INPUT_PATH" \
    --output "$OUTPUT_PATH/optimization" \
    --format "$FORMAT"

if [ $? -ne 0 ]; then
    echo "Warning: Spark optimization benchmarks failed."
fi

# Copy results from HDFS to local filesystem
echo "Copying results from HDFS to local filesystem..."
hadoop fs -copyToLocal "$OUTPUT_PATH/trends/*" "$LOCAL_OUTPUT_DIR/trends/"
hadoop fs -copyToLocal "$OUTPUT_PATH/bias_detection/*" "$LOCAL_OUTPUT_DIR/bias_detection/"
hadoop fs -copyToLocal "$OUTPUT_PATH/statistics/*" "$LOCAL_OUTPUT_DIR/statistics/"
hadoop fs -copyToLocal "$OUTPUT_PATH/optimization/*" "$LOCAL_OUTPUT_DIR/optimization/"

echo "Spark analysis completed successfully!"
echo "Results saved to HDFS: $OUTPUT_PATH"
echo "Results copied to local directory: $LOCAL_OUTPUT_DIR"

# If using virtual environment, deactivate it
if [ -d "$PROJECT_ROOT/venv_py310" ]; then
    deactivate
fi