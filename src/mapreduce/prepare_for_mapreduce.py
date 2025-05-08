from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
import logging
import os
import subprocess

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/mapreduce_preparation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

def create_spark_session():
    """Create and return a Spark session"""
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName("Amazon Reviews MapReduce Preparation") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    logger.info(f"Created Spark session: {spark.version}")
    return spark

def prepare_product_rating_data(spark, category):
    """Prepare data for product rating analysis"""
    input_path = f"/user/amazon_reviews/processed/{category}_processed"
    output_path = f"/user/amazon_reviews/mapreduce/{category}_product_ratings"
    
    logger.info(f"Preparing product rating data for {category}...")
    
    try:
        # Load processed data
        df = spark.read.parquet(input_path)
        
        # Select and transform relevant columns
        product_ratings = df.select(
            col("product_id"),
            col("rating"),
            col("review_date")
        )
        
        # Save in an optimized format for MapReduce
        product_ratings.write.json(output_path, mode="overwrite")
        
        logger.info(f"Saved product rating data to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error preparing product rating data for {category}: {str(e)}")
        return False

def prepare_time_analysis_data(spark, category):
    """Prepare data for time-based analysis"""
    input_path = f"/user/amazon_reviews/processed/{category}_processed"
    output_path = f"/user/amazon_reviews/mapreduce/{category}_time_analysis"
    
    logger.info(f"Preparing time analysis data for {category}...")
    
    try:
        # Load processed data
        df = spark.read.parquet(input_path)
        
        # Convert dates and add time components
        if "review_date" in df.columns:
            time_analysis = df.select(
                col("product_id"),
                col("rating"),
                col("review_date")
            )
            
            # Add year, month columns if date is available
            time_analysis = time_analysis.withColumn(
                "year", year(col("review_date"))
            ).withColumn(
                "month", month(col("review_date"))
            ).withColumn(
                "day", dayofmonth(col("review_date"))
            )
            
            # Save in an optimized format for MapReduce
            time_analysis.write.json(output_path, mode="overwrite")
            
            logger.info(f"Saved time analysis data to {output_path}")
            return True
        else:
            logger.error(f"No review_date column found in {category} data")
            return False
    except Exception as e:
        logger.error(f"Error preparing time analysis data for {category}: {str(e)}")
        return False

def prepare_review_length_data(spark, category):
    """Prepare data for review length analysis"""
    input_path = f"/user/amazon_reviews/processed/{category}_processed"
    output_path = f"/user/amazon_reviews/mapreduce/{category}_review_length"
    
    logger.info(f"Preparing review length data for {category}...")
    
    try:
        # Load processed data
        df = spark.read.parquet(input_path)
        
        # Select relevant columns
        review_length = df.select(
            col("product_id"),
            col("rating"),
            col("review_length"),
            col("category")
        )
        
        # Save in an optimized format for MapReduce
        review_length.write.json(output_path, mode="overwrite")
        
        logger.info(f"Saved review length data to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error preparing review length data for {category}: {str(e)}")
        return False

def prepare_all_data(spark):
    """Prepare all data for different MapReduce analyses"""
    categories = ["electronics", "toys_games", "cell_phones", "all_reviews"]
    
    # Ensure HDFS directories exist
    subprocess.run("hdfs dfs -mkdir -p /user/amazon_reviews/mapreduce", shell=True)
    
    # Prepare specific datasets for each analysis type
    for category in categories:
        prepare_product_rating_data(spark, category)
        prepare_time_analysis_data(spark, category)
        prepare_review_length_data(spark, category)
    
    logger.info("All data prepared for MapReduce analysis")

if __name__ == "__main__":
    try:
        logger.info("Starting MapReduce data preparation...")
        spark = create_spark_session()
        prepare_all_data(spark)
        spark.stop()
        logger.info("MapReduce data preparation completed")
    except Exception as e:
        logger.error(f"Error in MapReduce data preparation: {str(e)}")