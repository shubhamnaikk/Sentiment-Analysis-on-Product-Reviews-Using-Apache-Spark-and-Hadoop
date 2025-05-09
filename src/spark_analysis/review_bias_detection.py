#!/usr/bin/env python3
"""
Review bias detection - analyzing correlation between review length and ratings.
This script investigates if review length correlates with rating scores,
which might indicate bias in the review process.
"""

import argparse
import logging
import sys
import os
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, length, count, avg, stddev, corr, round, min, max
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.spark_analysis.spark_utils import (
    create_spark_session,
    load_amazon_reviews,
    preprocess_reviews_dataframe,
    save_results,
    optimize_dataframe
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_review_length_correlation(df, output_dir):
    """
    Analyze correlation between review length and ratings.
    
    Args:
        df (DataFrame): Preprocessed DataFrame containing reviews
        output_dir (str): Directory to save results
        
    Returns:
        dict: Dictionary containing results of the correlation analysis
    """
    logger.info("Starting review length correlation analysis")
    
    # Ensure we have the review_length column
    if "review_length" not in df.columns:
        logger.warning("No 'review_length' column found - calculating it now")
        df = df.withColumn("review_length", length(col("reviewText")))
    
    # Register the DataFrame as a temporary view for Spark SQL
    df.createOrReplaceTempView("reviews_with_length")
    
    # Get the Spark session from the DataFrame
    spark = df.sparkSession
    
    # Calculate average review length by rating
    length_by_rating = df.groupBy("overall") \
        .agg(
            avg("review_length").alias("avg_review_length"),
            stddev("review_length").alias("stddev_review_length"),
            min("review_length").alias("min_review_length"),
            max("review_length").alias("max_review_length"),
            count("*").alias("count")
        ) \
        .withColumn("rounded_avg_length", round(col("avg_review_length"), 2)) \
        .orderBy("overall")
    
    # Calculate simple correlation between review length and rating
    correlation_result = df.select(
        corr("review_length", "overall").alias("correlation")
    )
    
    # Get the correlation value
    correlation_value = correlation_result.collect()[0]["correlation"]
    logger.info(f"Correlation between review length and rating: {correlation_value}")
    
    # Create a DataFrame to store the correlation value
    correlation_df = spark.createDataFrame(
        [{"correlation_type": "review_length_vs_rating", "correlation_value": correlation_value}]
    )
    
    # For a more detailed analysis, use Spark ML for correlation analysis
    assembler = VectorAssembler(
        inputCols=["review_length", "overall"], 
        outputCol="features"
    )
    vector_df = assembler.transform(df)
    
    # Calculate correlation matrix
    corr_matrix = Correlation.corr(vector_df, "features")
    
    # Get the matrix as a numpy array for further processing if needed
    matrix_as_array = corr_matrix.collect()[0]["pearson(features)"].toArray()
    
    # Group reviews into length buckets
    df = df.withColumn(
        "length_bucket",
        (col("review_length") / 100).cast("integer") * 100
    )
    
    length_bucket_stats = df.groupBy("length_bucket") \
        .agg(
            avg("overall").alias("avg_rating"),
            count("*").alias("count")
        ) \
        .withColumn("rounded_avg_rating", round(col("avg_rating"), 2)) \
        .orderBy("length_bucket")
    
    # Save the results
    save_results(length_by_rating, f"{output_dir}/length_by_rating", "csv")
    save_results(correlation_df, f"{output_dir}/length_rating_correlation", "csv")
    save_results(length_bucket_stats, f"{output_dir}/length_bucket_stats", "csv")
    
    logger.info("Review length correlation analysis completed")
    
    return {
        "length_by_rating": length_by_rating,
        "correlation": correlation_value,
        "correlation_df": correlation_df,
        "length_bucket_stats": length_bucket_stats,
        "correlation_matrix": matrix_as_array
    }

def analyze_category_bias(df, output_dir):
    """
    Analyze if review bias varies by product category.
    
    Args:
        df (DataFrame): Preprocessed DataFrame containing reviews
        output_dir (str): Directory to save results
        
    Returns:
        DataFrame: Results of the category bias analysis
    """
    logger.info("Starting category bias analysis")
    
    # Ensure we have category information
    if "category" not in df.columns:
        logger.warning("No 'category' column found - skipping category bias analysis")
        return None
    
    # Ensure we have the review_length column
    if "review_length" not in df.columns:
        df = df.withColumn("review_length", length(col("reviewText")))
    
    # Register the DataFrame as a temporary view for Spark SQL
    df.createOrReplaceTempView("reviews_with_length")
    
    # Get the Spark session from the DataFrame
    spark = df.sparkSession
    
    # Get top 10 categories by review count
    top_categories = spark.sql("""
        SELECT 
            category, 
            COUNT(*) as review_count
        FROM reviews_with_length
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY review_count DESC
        LIMIT 10
    """)
    
    # Get list of top category values
    top_category_list = [row["category"] for row in top_categories.collect()]
    
    # Calculate correlation by category for top categories
    category_correlations = []
    
    for category in top_category_list:
        # Filter for the current category
        category_df = df.filter(col("category") == category)
        
        # Calculate correlation for this category
        correlation = category_df.select(
            corr("review_length", "overall").alias("correlation")
        ).collect()[0]["correlation"]
        
        category_correlations.append({
            "category": category,
            "correlation": correlation
        })
    
    # Create a DataFrame from the correlations
    category_correlation_df = spark.createDataFrame(category_correlations)
    
    # Calculate average length and rating by category
    category_stats = df.groupBy("category") \
        .agg(
            avg("review_length").alias("avg_review_length"),
            avg("overall").alias("avg_rating"),
            count("*").alias("review_count")
        ) \
        .withColumn("rounded_avg_length", round(col("avg_review_length"), 2)) \
        .withColumn("rounded_avg_rating", round(col("avg_rating"), 2)) \
        .orderBy(col("review_count").desc())
    
    # Save the results
    save_results(category_correlation_df, f"{output_dir}/category_correlations", "csv")
    save_results(category_stats, f"{output_dir}/category_stats", "csv")
    
    logger.info("Category bias analysis completed")
    
    return {
        "category_correlations": category_correlation_df,
        "category_stats": category_stats
    }

def analyze_verified_purchase_bias(df, output_dir):
    """
    Analyze if there's a bias in ratings for verified purchases vs. non-verified purchases.
    
    Args:
        df (DataFrame): Preprocessed DataFrame containing reviews
        output_dir (str): Directory to save results
        
    Returns:
        DataFrame: Results of the verified purchase bias analysis
    """
    logger.info("Starting verified purchase bias analysis")
    
    # Ensure we have verified purchase information
    if "verified" not in df.columns:
        logger.warning("No 'verified' column found - skipping verified purchase bias analysis")
        return None
    
    # Calculate rating statistics by verified status
    verified_stats = df.groupBy("verified") \
        .agg(
            avg("overall").alias("avg_rating"),
            avg("review_length").alias("avg_review_length"),
            count("*").alias("review_count")
        ) \
        .withColumn("rounded_avg_rating", round(col("avg_rating"), 2)) \
        .withColumn("rounded_avg_length", round(col("avg_review_length"), 2))
    
    # Save the results
    save_results(verified_stats, f"{output_dir}/verified_purchase_stats", "csv")
    
    logger.info("Verified purchase bias analysis completed")
    
    return verified_stats

def main():
    """Main function to run the review bias detection analysis."""
    parser = argparse.ArgumentParser(description="Analyze bias in Amazon reviews")
    parser.add_argument("--input", required=True, help="Input data path (HDFS or local)")
    parser.add_argument("--output", required=True, help="Output directory for results")
    parser.add_argument("--format", default="parquet", help="Input data format (parquet, json, csv)")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("Amazon Reviews Bias Detection")
    
    try:
        # Load data
        reviews_df = load_amazon_reviews(spark, args.input, args.format)
        
        # Preprocess the data
        processed_df = preprocess_reviews_dataframe(reviews_df)
        
        # Optimize the DataFrame
        optimized_df = optimize_dataframe(processed_df)
        
        # Analyze review length correlation with ratings
        analyze_review_length_correlation(optimized_df, args.output)
        
        # Analyze category bias
        analyze_category_bias(optimized_df, args.output)
        
        # Analyze verified purchase bias
        analyze_verified_purchase_bias(optimized_df, args.output)
        
        logger.info(f"Analysis completed. Results saved to {args.output}")
        
    except Exception as e:
        logger.error(f"Error in review bias detection: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # Stop the Spark session
        spark.stop()
        
if __name__ == "__main__":
    main()