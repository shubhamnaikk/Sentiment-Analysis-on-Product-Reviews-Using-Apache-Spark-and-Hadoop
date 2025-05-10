#!/bin/bash
# scripts/spark/run_spark_analysis.sh

# Set up environment variables
export PYTHONPATH=$(pwd):$PYTHONPATH
export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.5/libexec  # Adjust this to your Spark installation path

# Create log directory if it doesn't exist
mkdir -p logs

# Check if Spark is available
if [ ! -d "$SPARK_HOME" ]; then
    echo "Error: Spark installation not found at $SPARK_HOME"
    echo "Please install Spark or set the correct SPARK_HOME path"
    exit 1
fi

# Run the Spark analysis
echo "Starting Spark analysis..."
python3 src/spark/jobs/run_analysis.py

# Check the exit code
if [ $? -eq 0 ]; then
    echo "Spark analysis completed successfully!"
    echo "Results are available in data/spark_results/"
    
    # Check if matplotlib and pandas are installed for visualization
    python3 -c "import matplotlib.pyplot; import pandas" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "Creating visualizations..."
        python3 -c "
import sys
sys.path.append('$(pwd)')
from src.spark.utils.visualization_helper import save_trend_visualizations, save_bias_visualizations, save_stats_visualizations
import glob
import json
import os

# Create visualization directories
os.makedirs('data/spark_results/visualizations', exist_ok=True)

# Function to load results
def load_results(path_pattern, result_type):
    results = {}
    paths = glob.glob(path_pattern)
    for path in paths:
        category = path.split('/')[-3]  # Extract category from path
        if category not in results:
            results[category] = {}
        
        # Load JSON data
        with open(path, 'r') as f:
            try:
                data = json.load(f)
                results[category][result_type] = data
            except:
                print(f'Error loading {path}')
    
    return results

# Create visualizations for each type of analysis
categories = ['electronics', 'cell_phones', 'toys_games', 'all_categories']

for category in categories:
    # Try to create trend visualizations
    trend_paths = f'data/spark_results/{category}/trends/*_json/part-*.json'
    trend_results = load_results(trend_paths, 'trend')
    if category in trend_results and 'trend' in trend_results[category]:
        save_trend_visualizations(trend_results[category]['trend'], f'data/spark_results/visualizations/{category}/trends')
    
    # Try to create bias visualizations
    bias_paths = f'data/spark_results/{category}/bias/*_json/part-*.json'
    bias_results = load_results(bias_paths, 'bias')
    if category in bias_results and 'bias' in bias_results[category]:
        save_bias_visualizations(bias_results[category]['bias'], f'data/spark_results/visualizations/{category}/bias')
    
    # Try to create stats visualizations
    stats_paths = f'data/spark_results/{category}/stats/*_json/part-*.json'
    stats_results = load_results(stats_paths, 'stats')
    if category in stats_results and 'stats' in stats_results[category]:
        save_stats_visualizations(stats_results[category]['stats'], f'data/spark_results/visualizations/{category}/stats')

print('Visualization creation completed.')
"
    else
        echo "Matplotlib or Pandas not installed. Skipping visualization creation."
        echo "Install them with: pip install matplotlib pandas"
    fi
else
    echo "Spark analysis failed. Check logs for details."
    exit 1
fi

echo "All tasks completed."