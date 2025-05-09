#!/usr/bin/env python3
"""
Trend analysis of ratings over time using Spark SQL.
Analyzes how product ratings have changed over time.
"""

import argparse
import logging
import sys
import os
import pandas as pd
from pyspark.sql.functions import col, year, month, avg, count, round

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

def analyze_ratings_trend(df, output_dir):
    """
    Analyze trends in ratings over time.
    
    Args:
        df (DataFrame): Preprocessed DataFrame containing reviews
        output_dir (str): Directory to save results
        
    Returns:
        DataFrame: Results of the trend analysis
    """
    logger.info("Starting ratings trend analysis")
    
    # Register the DataFrame as a temporary view for Spark SQL
    df.createOrReplaceTempView("amazon_reviews")
    
    # Get the Spark session from the DataFrame
    spark = df.sparkSession
    
    # SQL approach - monthly trend analysis
    monthly_trend = spark.sql("""
        SELECT 
            review_year as year,
            review_month as month,
            AVG(overall) as avg_rating,
            COUNT(*) as review_count,
            ROUND(AVG(overall), 2) as rounded_avg_rating
        FROM amazon_reviews
        WHERE review_year IS NOT NULL AND review_month IS NOT NULL
        GROUP BY review_year, review_month
        ORDER BY review_year, review_month
    """)
    
    # DataFrame API approach - yearly trend analysis
    yearly_trend = df.groupBy("review_year") \
        .agg(
            avg("overall").alias("avg_rating"),
            count("*").alias("review_count")
        ) \
        .withColumn("rounded_avg_rating", round(col("avg_rating"), 2)) \
        .orderBy("review_year")
    
    # DataFrame API approach - quarterly trend analysis
    df = df.withColumn("quarter", ((col("review_month") - 1) / 3 + 1).cast("integer"))
    
    quarterly_trend = df.groupBy("review_year", "quarter") \
        .agg(
            avg("overall").alias("avg_rating"),
            count("*").alias("review_count")
        ) \
        .withColumn("rounded_avg_rating", round(col("avg_rating"), 2)) \
        .orderBy("review_year", "quarter")
    
    # Save the results
    save_results(monthly_trend, f"{output_dir}/monthly_ratings_trend", "csv")
    save_results(yearly_trend, f"{output_dir}/yearly_ratings_trend", "csv")
    save_results(quarterly_trend, f"{output_dir}/quarterly_ratings_trend", "csv")
    
    logger.info("Ratings trend analysis completed")
    return {
        "monthly": monthly_trend,
        "yearly": yearly_trend,
        "quarterly": quarterly_trend
    }

def analyze_category_rating_trends(df, output_dir):
    """
    Analyze rating trends by product category.
    
    Args:
        df (DataFrame): Preprocessed DataFrame containing reviews
        output_dir (str): Directory to save results
        
    Returns:
        DataFrame: Results of the category trend analysis
    """
    logger.info("Starting category-based ratings trend analysis")
    
    # Ensure we have category information
    if "category" not in df.columns:
        logger.warning("No 'category' column found - skipping category analysis")
        return None
    
    # Register the DataFrame as a temporary view for Spark SQL
    df.createOrReplaceTempView("amazon_reviews")
    
    # Get the Spark session from the DataFrame
    spark = df.sparkSession
    
    # SQL approach - yearly trend by category
    category_yearly_trend = spark.sql("""
        SELECT 
            category,
            review_year as year,
            AVG(overall) as avg_rating,
            COUNT(*) as review_count,
            ROUND(AVG(overall), 2) as rounded_avg_rating
        FROM amazon_reviews
        WHERE review_year IS NOT NULL AND category IS NOT NULL
        GROUP BY category, review_year
        ORDER BY category, review_year
    """)
    
    # Find top 10 categories by review count
    top_categories = spark.sql("""
        SELECT 
            category, 
            COUNT(*) as review_count
        FROM amazon_reviews
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY review_count DESC
        LIMIT 10
    """)
    
    # Get list of top category values
    top_category_list = [row["category"] for row in top_categories.collect()]
    
    # Filter the yearly trend for top categories only
    top_category_yearly_trend = category_yearly_trend.filter(
        col("category").isin(top_category_list)
    )
    
    # Save the results
    save_results(category_yearly_trend, f"{output_dir}/category_yearly_ratings_trend", "csv")
    save_results(top_categories, f"{output_dir}/top_categories_by_review_count", "csv")
    save_results(top_category_yearly_trend, f"{output_dir}/top_category_yearly_ratings_trend", "csv")
    
    logger.info("Category-based ratings trend analysis completed")
    return {
        "category_yearly": category_yearly_trend,
        "top_categories": top_categories,
        "top_category_yearly": top_category_yearly_trend
    }

def main():
    """Main function to run the ratings trend analysis."""
    parser = argparse.ArgumentParser(description="Analyze Amazon reviews rating trends over time")
    parser.add_argument("--input", required=True, help="Input data path (HDFS or local)")
    parser.add_argument("--output", required=True, help="Output directory for results")
    parser.add_argument("--format", default="parquet", help="Input data format (parquet, json, csv)")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("Amazon Reviews Rating Trends Analysis")
    
    try:
        # Load data
        reviews_df = load_amazon_reviews(spark, args.input, args.format)
        
        # Preprocess the data
        processed_df = preprocess_reviews_dataframe(reviews_df)
        
        # Optimize the DataFrame
        optimized_df = optimize_dataframe(processed_df)
        
        # Analyze ratings trend
        analyze_ratings_trend(optimized_df, args.output)
        
        # Analyze category-based trends
        analyze_category_rating_trends(optimized_df, args.output)
        
        logger.info(f"Analysis completed. Results saved to {args.output}")
        
    except Exception as e:
        logger.error(f"Error in ratings trend analysis: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # Stop the Spark session
        spark.stop()
        
if __name__ == "__main__":
    main()