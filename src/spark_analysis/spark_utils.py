#!/usr/bin/env python3
"""
Utility functions for Spark analysis tasks.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="Amazon Reviews Analysis", configs=None):
    """
    Create and configure a Spark session.
    
    Args:
        app_name (str): Name of the Spark application
        configs (dict): Additional Spark configurations
        
    Returns:
        SparkSession: Configured SparkSession object
    """
    logger.info(f"Creating Spark session for {app_name}")
    
    # Start with the basic builder
    builder = SparkSession.builder.appName(app_name)
    
    # Add memory configurations for better performance
    builder = builder.config("spark.memory.offHeap.enabled", "true") \
                    .config("spark.memory.offHeap.size", "2g") \
                    .config("spark.sql.shuffle.partitions", "10") \
                    .config("spark.executor.memory", "4g")
    
    # Add any additional configurations
    if configs:
        for key, value in configs.items():
            builder = builder.config(key, value)
    
    # Create the session
    spark = builder.getOrCreate()
    
    logger.info(f"Spark session created: {spark.version}")
    return spark

def load_amazon_reviews(spark, data_path, format_type="parquet"):
    """
    Load Amazon reviews dataset into a Spark DataFrame.
    
    Args:
        spark (SparkSession): Active Spark session
        data_path (str): Path to the data (HDFS or local)
        format_type (str): Format of the data (parquet, json, csv)
        
    Returns:
        DataFrame: Spark DataFrame containing the reviews
    """
    logger.info(f"Loading Amazon reviews data from {data_path}")
    
    if format_type.lower() == "parquet":
        df = spark.read.parquet(data_path)
    elif format_type.lower() == "json":
        df = spark.read.json(data_path)
    elif format_type.lower() == "csv":
        df = spark.read.option("header", "true").csv(data_path)
    else:
        raise ValueError(f"Unsupported format: {format_type}")
    
    logger.info(f"Loaded {df.count()} reviews")
    return df

def preprocess_reviews_dataframe(df):
    """
    Preprocess the reviews DataFrame for analysis.
    
    Args:
        df (DataFrame): Original reviews DataFrame
        
    Returns:
        DataFrame: Preprocessed DataFrame with additional columns
    """
    logger.info("Preprocessing reviews DataFrame")
    
    # Ensure we have all required columns
    required_columns = ["reviewText", "overall", "unixReviewTime", "reviewTime"]
    for column in required_columns:
        if column not in df.columns:
            logger.warning(f"Missing column: {column}")
    
    # Add review length column
    if "reviewText" in df.columns:
        df = df.withColumn("review_length", length(col("reviewText")))
    
    # Convert unix timestamp to date components if available
    if "unixReviewTime" in df.columns:
        df = df.withColumn("review_date", col("unixReviewTime").cast("timestamp"))
        df = df.withColumn("review_year", year(col("review_date")))
        df = df.withColumn("review_month", month(col("review_date")))
        df = df.withColumn("review_day", dayofmonth(col("review_date")))
    
    logger.info("DataFrame preprocessing completed")
    return df

def save_results(df, output_path, format_type="csv"):
    """
    Save analysis results to the specified location.
    
    Args:
        df (DataFrame): DataFrame containing results
        output_path (str): Path where results should be saved
        format_type (str): Format to save (csv, parquet, json)
    """
    logger.info(f"Saving results to {output_path} in {format_type} format")
    
    # Create directory if it doesn't exist (for local paths)
    if output_path.startswith("file:") or not (output_path.startswith("hdfs:") or output_path.startswith("s3:")):
        directory = os.path.dirname(output_path.replace("file:", ""))
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
    
    if format_type.lower() == "csv":
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    elif format_type.lower() == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif format_type.lower() == "json":
        df.coalesce(1).write.mode("overwrite").json(output_path)
    else:
        raise ValueError(f"Unsupported format: {format_type}")
    
    logger.info(f"Results saved successfully")

def optimize_dataframe(df, partition_count=None):
    """
    Optimize a DataFrame for better performance.
    
    Args:
        df (DataFrame): DataFrame to optimize
        partition_count (int): Number of partitions (None for automatic)
        
    Returns:
        DataFrame: Optimized DataFrame
    """
    logger.info("Optimizing DataFrame for better performance")
    
    # Repartition if needed
    if partition_count:
        df = df.repartition(partition_count)
    
    # Cache the DataFrame in memory
    df.cache()
    
    # Trigger an action to cache the data
    count = df.count()
    logger.info(f"DataFrame cached with {count} records")
    
    return df

def get_schema_for_amazon_reviews():
    """
    Get the schema for Amazon reviews dataset.
    
    Returns:
        StructType: Schema for Amazon reviews
    """
    return StructType([
        StructField("reviewerID", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("reviewerName", StringType(), True),
        StructField("helpful", StringType(), True),
        StructField("reviewText", StringType(), True),
        StructField("overall", FloatType(), True),
        StructField("summary", StringType(), True),
        StructField("unixReviewTime", IntegerType(), True),
        StructField("reviewTime", StringType(), True),
        StructField("category", StringType(), True)
    ])