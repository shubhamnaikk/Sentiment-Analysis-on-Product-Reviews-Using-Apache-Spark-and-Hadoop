#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_date, when, length, lit
import logging
import os
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and return a Spark session with retry logic"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            spark = SparkSession.builder \
                .appName("MapReduce Data Preparation") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.network.timeout", "800s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .getOrCreate()
            
            logger.info(f"Created Spark session: {spark.version}")
            return spark
        except Exception as e:
            retry_count += 1
            logger.error(f"Error creating Spark session (attempt {retry_count}/{max_retries}): {str(e)}")
            if retry_count >= max_retries:
                raise
            time.sleep(5)  # Wait before retrying

def prepare_data_safely(spark, category):
    """Prepare all data types for a category with error handling"""
    try:
        # Check if the spark session is still active
        if spark._jsc.sc().isStopped():
            logger.warning("Spark context stopped, recreating...")
            spark = create_spark_session()
        
        # Load the data only once per category
        input_path = f"/user/amazon_reviews/processed/{category}_processed"
        logger.info(f"Loading data for {category}...")
        
        try:
            df = spark.read.parquet(input_path)
            logger.info(f"Successfully loaded {df.count()} records for {category}")
            
            # Prepare product ratings
            prepare_product_ratings(spark, df, category)
            
            # Prepare time analysis
            prepare_time_analysis(spark, df, category)
            
            # Prepare review length
            prepare_review_length(spark, df, category)
        
        except Exception as e:
            logger.error(f"Error processing {category}: {str(e)}")
    
    except Exception as e:
        logger.error(f"Fatal error processing {category}: {str(e)}")

def prepare_product_ratings(spark, df, category):
    """Prepare product rating data for MapReduce"""
    output_path = f"/user/amazon_reviews/mapreduce/{category}_product_ratings"
    
    logger.info(f"Preparing product rating data for {category}...")
    
    try:
        # Select product_id and rating columns
        if "product_id" in df.columns and "rating" in df.columns:
            # Filter out rows with null ratings
            ratings_df = df.select("product_id", "rating") \
                          .filter(col("product_id").isNotNull() & col("rating").isNotNull())
            
            # Count records for logging
            count = ratings_df.count()
            logger.info(f"Found {count} product ratings for {category}")
            
            if count > 0:
                # Save as text file with tab delimiter for MapReduce
                ratings_df.write.mode("overwrite") \
                          .format("csv") \
                          .option("delimiter", "\t") \
                          .option("header", "false") \
                          .save(output_path)
                
                logger.info(f"Saved product ratings for {category}")
            else:
                logger.warning(f"No product ratings to save for {category}")
        else:
            logger.error(f"Required columns not found in {category} data")
            
    except Exception as e:
        logger.error(f"Error preparing product rating data for {category}: {str(e)}")

def prepare_time_analysis(spark, df, category):
    """Prepare time analysis data for MapReduce"""
    output_path = f"/user/amazon_reviews/mapreduce/{category}_time_analysis"
    
    logger.info(f"Preparing time analysis data for {category}...")
    
    try:
        # Check if details column exists
        if "details" in df.columns:
            # Extract date information
            date_df = df.withColumn(
                "extracted_date",
                regexp_extract(col("details"), "\"Date First Available\":\\s*\"([^\"]+)\"", 1)
            )
            
            # Convert extracted dates to standard format
            date_df = date_df.withColumn(
                "review_date", 
                to_date(col("extracted_date"), "MMMM d, yyyy")
            )
            
            # Select relevant columns and filter out rows with null dates
            time_df = date_df.select(
                "product_id", "review_date", "rating", "category"
            ).filter(col("review_date").isNotNull())
            
            # Check if we have any records with valid dates
            count = time_df.count()
            logger.info(f"Found {count} records with valid dates for {category}")
            
            if count > 0:
                # Save as text file with tab delimiter for MapReduce
                time_df.write.mode("overwrite") \
                       .format("csv") \
                       .option("delimiter", "\t") \
                       .option("header", "false") \
                       .save(output_path)
                
                logger.info(f"Saved time analysis data for {category}")
            else:
                logger.warning(f"No valid dates found in {category} data")
        else:
            logger.error(f"No details column found in {category} data")
            
    except Exception as e:
        logger.error(f"Error preparing time analysis data for {category}: {str(e)}")

def prepare_review_length(spark, df, category):
    """Prepare review length data for MapReduce"""
    output_path = f"/user/amazon_reviews/mapreduce/{category}_review_length"
    
    logger.info(f"Preparing review length data for {category}...")
    
    try:
        # Calculate lengths for available text fields
        length_df = df
        
        # Add description length if description column exists
        if "description" in df.columns:
            length_df = length_df.withColumn(
                "description_length", 
                when(col("description").isNotNull(), length(col("description"))).otherwise(0)
            )
        else:
            length_df = length_df.withColumn("description_length", lit(0))
        
        # Add features length if features column exists
        if "features" in df.columns:
            length_df = length_df.withColumn(
                "features_length", 
                when(col("features").isNotNull(), length(col("features"))).otherwise(0)
            )
        else:
            length_df = length_df.withColumn("features_length", lit(0))
        
        # Add title length
        if "title" in df.columns:
            length_df = length_df.withColumn(
                "title_length", 
                when(col("title").isNotNull(), length(col("title"))).otherwise(0)
            )
        else:
            length_df = length_df.withColumn("title_length", lit(0))
        
        # Create a composite review_length field
        length_df = length_df.withColumn(
            "review_length",
            when(col("description_length") > 0, col("description_length"))
            .otherwise(
                when(col("features_length") > 0, col("features_length"))
                .otherwise(
                    when(col("title_length") > 0, col("title_length"))
                    .otherwise(0)
                )
            )
        )
        
        # Select relevant columns and filter out rows with zero length
        review_length_df = length_df.select(
            "product_id", "review_length", "rating", "category"
        ).filter(col("review_length") > 0)
        
        # Check if we have any records with valid lengths
        count = review_length_df.count()
        logger.info(f"Found {count} records with valid lengths for {category}")
        
        if count > 0:
            # Save as text file with tab delimiter for MapReduce
            review_length_df.write.mode("overwrite") \
                          .format("csv") \
                          .option("delimiter", "\t") \
                          .option("header", "false") \
                          .save(output_path)
            
            logger.info(f"Saved review length data for {category}")
        else:
            logger.warning(f"No valid lengths found in {category} data")
            
    except Exception as e:
        logger.error(f"Error preparing review length data for {category}: {str(e)}")

def main():
    """Main function"""
    logger.info("Starting MapReduce data preparation...")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Prepare data for each category
        categories = ["electronics", "toys_games", "cell_phones", "all_reviews"]
        
        for category in categories:
            prepare_data_safely(spark, category)
        
        logger.info("All data prepared for MapReduce analysis")
        
        # Stop Spark session
        logger.info("Stopping Spark session...")
        if spark and not spark._jsc.sc().isStopped():
            spark.stop()
        
        logger.info("MapReduce data preparation completed")
    
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")

if __name__ == "__main__":
    main()