#!/bin/bash
# integrate_data.sh

echo "Integrating Person 1's data with Person 2's MapReduce pipeline..."

# Define paths - use parent directory since we're in scripts/
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_PROCESSED_DIR="$PROJECT_ROOT/data/processed"
HDFS_BASE_DIR="/user/amazon_reviews"
HDFS_MAPREDUCE_DIR="$HDFS_BASE_DIR/mapreduce"

echo "Project root: $PROJECT_ROOT"
echo "Processing data from: $LOCAL_PROCESSED_DIR"

# Create HDFS directories if they don't exist
echo "Creating HDFS directories..."
hadoop fs -mkdir -p $HDFS_BASE_DIR
hadoop fs -mkdir -p $HDFS_MAPREDUCE_DIR

# Process each category
categories=("Electronics" "Cell_Phones" "Toys_Games")

for category in "${categories[@]}"; do
    echo "Processing $category..."
    json_file="$LOCAL_PROCESSED_DIR/$category.json"
    
    # Convert category name to lowercase with underscore for compatibility
    lowercase_category=$(echo $category | tr 'A-Z' 'a-z' | tr ' ' '_')
    
    # Define the path for ratings data in HDFS
    hdfs_ratings_path="$HDFS_MAPREDUCE_DIR/${lowercase_category}_product_ratings"
    
    # Remove existing HDFS directory if it exists
    hadoop fs -rm -r -f $hdfs_ratings_path
    
    # Create temporary file to hold formatted data
    temp_file=$(mktemp)
    
    echo "Converting $json_file to MapReduce format..."
    # Now that we know the files are gzipped JSON Lines, use the right approach
    python3 -c "
import json
import gzip
import os

file_path = '$json_file'
print(f'Processing file: {file_path}')
print(f'File exists: {os.path.exists(file_path)}')

try:
    # Process as gzipped JSON Lines
    count = 0
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        with open('$temp_file', 'w') as outfile:
            for line in f:
                try:
                    item = json.loads(line.strip())
                    # Extract product_id (from parent_asin) and average_rating
                    if 'parent_asin' in item and 'average_rating' in item:
                        product_id = str(item['parent_asin']).replace('\\t', ' ').replace('\\n', ' ')
                        rating = float(item['average_rating'])
                        category = '$lowercase_category'
                        outfile.write(f'{product_id}\\t{rating}\\n')
                        count += 1
                except (json.JSONDecodeError, ValueError) as e:
                    print(f'Error processing line: {e}')
                    continue
    
    print(f'Wrote {count} formatted records to temp file')
except Exception as e:
    import traceback
    print(f'Error processing file: {e}')
    traceback.print_exc()
"
    
    # Check if the temp file has data
    if [ -s "$temp_file" ]; then
        # Upload to HDFS
        echo "Uploading to HDFS at $hdfs_ratings_path..."
        hadoop fs -put $temp_file $hdfs_ratings_path
        
        echo "Completed processing $category successfully"
    else
        echo "WARNING: No data was extracted from $category. Temp file is empty."
    fi
    
    # Clean up temp file
    rm $temp_file
    echo "--------------------------------------"
done

# Check if any data was uploaded to HDFS
echo "Checking if MapReduce input data exists..."
hadoop fs -ls $HDFS_MAPREDUCE_DIR

# Create all_reviews combined data if any data was uploaded
echo "Creating combined all_reviews dataset..."
hdfs_all_reviews_path="$HDFS_MAPREDUCE_DIR/all_reviews_product_ratings"
hadoop fs -rm -r -f $hdfs_all_reviews_path

# Combine all category data into all_reviews if they exist
if hadoop fs -test -e "$HDFS_MAPREDUCE_DIR/electronics_product_ratings" && \
   hadoop fs -test -e "$HDFS_MAPREDUCE_DIR/cell_phones_product_ratings" && \
   hadoop fs -test -e "$HDFS_MAPREDUCE_DIR/toys_games_product_ratings"; then
   
    echo "Combining all category data into all_reviews..."
    hadoop fs -cat "$HDFS_MAPREDUCE_DIR/electronics_product_ratings" \
               "$HDFS_MAPREDUCE_DIR/cell_phones_product_ratings" \
               "$HDFS_MAPREDUCE_DIR/toys_games_product_ratings" | \
    hadoop fs -put - "$hdfs_all_reviews_path"
    
    echo "All_reviews dataset created successfully!"
else
    echo "Skipping all_reviews creation as some category files don't exist."
fi

echo "Data integration complete!"
echo "You can now run your MapReduce jobs using run_mapreduce_jobs.sh"