#!/bin/bash
# scripts/spark/integrate_results.sh

# This script combines the results from MapReduce (Person 2) and Spark (Person 3)
# to create a comprehensive analysis report

echo "Integrating MapReduce and Spark analysis results..."

# Create the integration directory
INTEGRATION_DIR="data/integrated_results"
mkdir -p $INTEGRATION_DIR

# Copy the MapReduce top products results
echo "Copying MapReduce top products results..."
MAPREDUCE_FILES=(
    "electronics_top_analysis_results.txt"
    "cell_phones_top_analysis_results.txt"
    "toys_games_top_analysis_results.txt"
    "all_reviews_top_analysis_results.txt"
)

for file in "${MAPREDUCE_FILES[@]}"; do
    if [ -f "$file" ]; then
        cp "$file" "$INTEGRATION_DIR/"
        echo "  Copied $file"
    else
        echo "  Warning: $file not found"
    fi
done

# Copy Spark analysis results (summary files only)
echo "Copying Spark analysis results..."
SPARK_DIRS=(
    "electronics"
    "cell_phones"
    "toys_games"
    "all_categories"
)

for dir in "${SPARK_DIRS[@]}"; do
    # Copy statistical summaries
    if [ -f "data/spark_results/$dir/stats/basic_stats_summary.txt" ]; then
        cp "data/spark_results/$dir/stats/basic_stats_summary.txt" "$INTEGRATION_DIR/${dir}_stats_summary.txt"
        echo "  Copied ${dir}_stats_summary.txt"
    fi
    
    # Copy bias analysis correlation
    if [ -f "data/spark_results/$dir/bias/correlation.txt" ]; then
        cp "data/spark_results/$dir/bias/correlation.txt" "$INTEGRATION_DIR/${dir}_bias_correlation.txt"
        echo "  Copied ${dir}_bias_correlation.txt"
    fi
    
    # Copy visualizations if they exist
    VISUALIZATION_DIR="data/spark_results/visualizations/$dir"
    if [ -d "$VISUALIZATION_DIR" ]; then
        mkdir -p "$INTEGRATION_DIR/visualizations/$dir"
        cp -r "$VISUALIZATION_DIR"/* "$INTEGRATION_DIR/visualizations/$dir/"
        echo "  Copied $dir visualizations"
    fi
done

# Create integration summary
echo "Creating integration summary..."
cat > "$INTEGRATION_DIR/integration_summary.md" << 'EOL'
# Amazon Reviews Analysis - Integrated Results

This document summarizes the combined results from both the MapReduce and Spark analysis pipelines.

## MapReduce Analysis (Person 2)

The MapReduce pipeline identified top products and categories based on:
- Average ratings
- Popularity (number of reviews)

Results for each category are available in the respective text files:
- `electronics_top_analysis_results.txt`
- `cell_phones_top_analysis_results.txt`
- `toys_games_top_analysis_results.txt`
- `all_reviews_top_analysis_results.txt`

## Spark Analysis (Person 3)

The Spark analysis pipeline provided deeper insights into:

### 1. Ratings Distribution
Statistical analysis of ratings across all categories, including mean, median, variance, and distribution patterns.

### 2. Review Bias Detection
Analysis of correlation between review length and ratings to identify potential bias patterns.

### 3. Rating Trends Over Time
Analysis of how ratings have changed over time, identifying seasonal patterns and long-term trends.

## Key Insights

The combined analysis reveals:

1. Product Insights:
   - Top-rated products across categories
   - Most popular products by review count

2. Rating Patterns:
   - Distribution of ratings across star levels
   - Potential bias in review patterns
   - Temporal trends in customer satisfaction

3. Category Performance:
   - Relative performance of different product categories
   - Changes in category performance over time

## Visualizations

Visual representations of the analysis results are available in the `visualizations` directory, providing graphical insights into the findings.
EOL

echo "Integration completed. All results are in $INTEGRATION_DIR"
echo "Summary file: $INTEGRATION_DIR/integration_summary.md"