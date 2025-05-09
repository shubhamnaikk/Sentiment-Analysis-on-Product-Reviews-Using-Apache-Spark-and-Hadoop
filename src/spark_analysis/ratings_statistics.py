#!/usr/bin/env python3
"""
Basic statistical analysis of Amazon review ratings.
This script performs statistical analysis on ratings distribution.
"""

import argparse
import logging
import sys
import os
import pandas as pd
from pyspark.sql.functions import (
    col, count, avg, stddev, min, max, percentile_approx, 
    sum as spark_sum, round, expr, lit
)
from pyspark.sql.types import DoubleType

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

def analyze_rating_distribution(df, output_dir):
    """
    Analyze the overall distribution of ratings.
    
    Args:
        df (DataFrame): Preprocessed DataFrame containing reviews
        output_dir (str): Directory to save results
        
    Returns:
        dict: Dictionary containing results of the distribution analysis
    """
    logger.info("Starting rating distribution analysis")
    
    # Basic statistics of ratings
    rating_stats = df.select("overall").summary(
        "count", "min", "25%", "50%", "75%", "max", "mean", "stddev"
    )
    
    # Distribution of ratings
    rating_distribution = df.groupBy("overall") \
        .count() \
        .orderBy("overall")
    
    # Calculate percentage of each rating
    total_count = df.count()
    
    rating_distribution = rating_distribution \
        .withColumn("percentage", (col("count") / total_count) * 100) \
        .withColumn("rounded_percentage", round(col("percentage"), 2))
    
    # Rating sentiment categories
    df = df.withColumn(
        "sentiment_category",
        expr("""
            CASE
                WHEN overall <= 2.0 THEN 'Negative'
                WHEN overall = 3.0 THEN 'Neutral'
                WHEN overall >= 4.0 THEN 'Positive'
                ELSE 'Unknown'
            END
        """)
    )
    
    sentiment_distribution = df.groupBy("sentiment_category") \
        .count() \
        .withColumn("percentage", (col("count") / total_count) * 100) \
        .withColumn("rounded_percentage", round(col("percentage"), 2))
    
    # Save the results
    # Convert rating_stats to a more usable format
    stats_rows = rating_stats.collect()
    stats_dict = {row["summary"]: row.asDict() for row in stats_rows}
    
    # Convert to regular DataFrame for easier saving
    spark = df.sparkSession
    stats_df = spark.createDataFrame([{
        "metric": "count",
        "value": float(stats_dict["count"]["overall"])
    }, {
        "metric": "min",
        "value": float(stats_dict["min"]["overall"])
    }, {
        "metric": "max",
        "value": float(stats_dict["max"]["overall"])
    }, {
        "metric": "mean",
        "value": float(stats_dict["mean"]["overall"])
    }, {
        "metric": "stddev",
        "value": float(stats_dict["stddev"]["overall"])
    }, {
        "metric": "25th_percentile",
        "value": float(stats_dict["25%"]["overall"])
    }, {
        "metric": "median",
        "value": float(stats_dict["50%"]["overall"])
    }, {
        "metric": "75th_percentile",
        "value": float(stats_dict["75%"]["overall"])
    }])
    
    save_results(stats_df, f"{output_dir}/rating_stats", "csv")
    save_results(rating_distribution, f"{output_dir}/rating_distribution", "csv")
    save_results(sentiment_distribution, f"{output_dir}/sentiment_distribution", "csv")
    
    logger.info("Rating distribution analysis completed")
    
    return {
        "rating_stats": stats_df,
        "rating_distribution": rating_distribution,
        "sentiment_distribution": sentiment_distribution
    }

def analyze_category_ratings(df, output_dir):
    """
    Analyze ratings distribution by product category.
    
    Args:
        df (DataFrame): Preprocessed DataFrame containing reviews
        output_dir (str): Directory to save results
        
    Returns:
        DataFrame: Results of the category ratings analysis
    """
    logger.info("Starting category ratings analysis")
    
    # Ensure we have category information
    if "category" not in df.columns:
        logger.warning("No 'category' column found - skipping category ratings analysis")
        return None
    
    # Calculate rating statistics by category
    category_stats = df.groupBy("category") \
        .agg(
            count("*").alias("review_count"),
            avg("overall").alias("avg_rating"),
            stddev("overall").alias("stddev_rating"),
            min("overall").alias("min_rating"),
            max("overall").alias("max_rating")
        ) \
        .withColumn("rounded_avg_rating", round(col("avg_rating"), 2)) \
        .orderBy(col("review_count").desc())
    
    # Get top 10 highest rated categories (with minimum 100 reviews)
    top_rated_categories = category_stats \
        .filter(col("review_count") >= 100) \
        .orderBy(col("avg_rating").desc()) \
        .limit(10)
    
    # Get top 10 lowest rated categories (with minimum 100 reviews)
    lowest_rated_categories = category_stats \
        .filter(col("review_count") >= 100) \
        .orderBy(col("avg_rating").asc()) \
        .limit(10)
    
    # Get top 10 categories by review count
    top_categories_by_count = category_stats \
        .orderBy(col("review_count").desc()) \
        .limit(10)
    
    # Save the results
    save_results(category_stats, f"{output_dir}/category_rating_stats", "csv")
    save_results(top_rated_categories, f"{output_dir}/top_rated_categories", "csv")
    save_results(lowest_rated_categories, f"{output_dir}/lowest_rated_categories", "csv")
    save_results(top_categories_by_count, f"{output_dir}/top_categories_by_count", "csv")
    
    logger.info("Category ratings analysis completed")
    
    return {
        "category_stats": category_stats,
        "top_rated_categories": top_rated_categories,
        "lowest_rated_categories": lowest_rated_categories,
        "top_categories_by_count": top_categories_by_count
    }

def analyze_helpful_ratings(df, output_dir):
    """
    Analyze relationship between helpfulness and ratings.
    
    Args:
        df (DataFrame): Preprocessed DataFrame containing reviews
        output_dir (str): Directory to save results
        
    Returns:
        DataFrame: Results of the helpful ratings analysis
    """
    logger.info("Starting helpful ratings analysis")
    
    # Ensure we have helpful information
    if "helpful" not in df.columns:
        logger.warning("No 'helpful' column found - skipping helpful ratings analysis")
        return None
    
    # Process helpful column - typically in format [upvotes, total_votes]
    # This is highly dependent on data format, may need adjustment
    spark = df.sparkSession
    
    try:
        # Attempt to parse helpful column assuming format [upvotes, total_votes]
        df = df.withColumn("helpful_upvotes", expr("cast(split(regexp_replace(helpful, '\\\\[|\\\\]', ''), ',')[0] as int)"))
        df = df.withColumn("helpful_total", expr("cast(split(regexp_replace(helpful, '\\\\[|\\\\]', ''), ',')[1] as int)"))
        
        # Create helpfulness ratio
        df = df.withColumn(
            "helpfulness_ratio",
            when(col("helpful_total") > 0, col("helpful_upvotes") / col("helpful_total")).otherwise(0)
        )
        
        # Group by rating and calculate average helpfulness
        helpfulness_by_rating = df.groupBy("overall") \
            .agg(
                avg("helpfulness_ratio").alias("avg_helpfulness_ratio"),
                avg("helpful_upvotes").alias("avg_upvotes"),
                avg("helpful_total").alias("avg_total_votes"),
                count("*").alias("review_count")
            ) \
            .withColumn("rounded_avg_helpfulness", round(col("avg_helpfulness_ratio"), 3)) \
            .orderBy("overall")
        
        # Save the results
        save_results(helpfulness_by_rating, f"{output_dir}/helpfulness_by_rating", "csv")
        
        logger.info("Helpful ratings analysis completed")
        
        return helpfulness_by_rating
        
    except Exception as e:
        logger.error(f"Error processing helpful column: {str(e)}")
        logger.info("Skipping helpful ratings analysis due to error")
        return None

def main():
    """Main function to run the ratings statistics analysis."""
    parser = argparse.ArgumentParser(description="Analyze Amazon reviews ratings statistics")
    parser.add_argument("--input", required=True, help="Input data path (HDFS or local)")
    parser.add_argument("--output", required=True, help="Output directory for results")
    parser.add_argument("--format", default="parquet", help="Input data format (parquet, json, csv)")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("Amazon Reviews Rating Statistics")
    
    try:
        # Load data
        reviews_df = load_amazon_reviews(spark, args.input, args.format)
        
        # Preprocess the data
        processed_df = preprocess_reviews_dataframe(reviews_df)
        
        # Optimize the DataFrame
        optimized_df = optimize_dataframe(processed_df)
        
        # Analyze rating distribution
        analyze_rating_distribution(optimized_df, args.output)
        
        # Analyze category ratings
        analyze_category_ratings(optimized_df, args.output)
        
        # Analyze helpful ratings
        analyze_helpful_ratings(optimized_df, args.output)
        
        logger.info(f"Analysis completed. Results saved to {args.output}")
        
    except Exception as e:
        logger.error(f"Error in ratings statistics analysis: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # Stop the Spark session
        spark.stop()
        
if __name__ == "__main__":
    main()