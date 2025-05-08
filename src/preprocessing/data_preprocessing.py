from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, length, lit
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/data_preprocessing.log"),
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
        .appName("Amazon Reviews Preprocessing") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    logger.info(f"Created Spark session: {spark.version}")
    return spark

def load_data(spark, category):
    """Load data for a specific category from HDFS"""
    hdfs_path = f"/user/amazon_reviews/raw/{category}.json.gz"
    logger.info(f"Loading {category} data from HDFS: {hdfs_path}")
    
    df = spark.read.json(hdfs_path)
    logger.info(f"Loaded {df.count()} records for {category}")
    
    # Add category column
    df = df.withColumn("category", lit(category))
    
    return df

def preprocess_data(df, category):
    """Preprocess the Amazon reviews data"""
    logger.info(f"Preprocessing {category} data...")
    
    # Count initial records
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count}")
    
    # Select and rename relevant columns
    if "unixReviewTime" in df.columns:
        df = df.withColumn("review_date", from_unixtime(col("unixReviewTime"), "yyyy-MM-dd"))
    
    # Add review length column if reviewText exists
    if "reviewText" in df.columns:
        df = df.withColumn("review_length", length(col("reviewText")))
    
    # Remove rows with null values in critical fields
    df = df.dropna(subset=["asin", "overall"])
    
    # Remove duplicate reviews if all needed columns exist
    if all(col in df.columns for col in ["reviewerID", "asin", "unixReviewTime"]):
        df = df.dropDuplicates(["reviewerID", "asin", "unixReviewTime"])
    
    # Select and rename relevant columns
    df = df.select(
        col("reviewerID"),
        col("asin").alias("product_id"),
        col("overall").alias("rating"),
        col("reviewText"),
        col("review_length") if "review_length" in df.columns else length(col("reviewText")).alias("review_length"),
        col("summary") if "summary" in df.columns else lit(None).alias("summary"),
        col("review_date") if "review_date" in df.columns else lit(None).alias("review_date"),
        col("verified") if "verified" in df.columns else lit(None).alias("verified"),
        col("category")
    )
    
    # Count final records
    final_count = df.count()
    logger.info(f"Final record count: {final_count}")
    logger.info(f"Removed {initial_count - final_count} records during preprocessing")
    
    return df

def process_all_categories(spark):
    """Process all categories and save the results"""
    categories = ["Electronics", "Toys_Games", "Cell_Phones"]
    
    dfs = []
    for category in categories:
        try:
            df = load_data(spark, category)
            df = preprocess_data(df, category.lower())
            dfs.append(df)
            
            # Save individual category data
            hdfs_category_path = f"/user/amazon_reviews/processed/{category.lower()}_processed"
            logger.info(f"Saving {category} processed data to {hdfs_category_path}")
            df.write.parquet(hdfs_category_path, mode="overwrite")
            
        except Exception as e:
            logger.error(f"Error processing {category}: {str(e)}")
    
    if len(dfs) > 0:
        # Combine all dataframes
        logger.info("Combining all categories...")
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.union(df)
        
        # Save the processed data
        hdfs_output_path = "/user/amazon_reviews/processed/all_reviews_processed"
        logger.info(f"Saving combined processed data to {hdfs_output_path}")
        combined_df.write.parquet(hdfs_output_path, mode="overwrite")
        
        logger.info(f"Combined dataset has {combined_df.count()} records")
    else:
        logger.error("No dataframes to combine, skipping combined output")
    
    logger.info("Data preprocessing completed")

if __name__ == "__main__":
    try:
        logger.info("Starting data preprocessing pipeline...")
        spark = create_spark_session()
        process_all_categories(spark)
        spark.stop()
        logger.info("Data preprocessing pipeline completed successfully")
    except Exception as e:
        logger.error(f"Error in preprocessing pipeline: {str(e)}")