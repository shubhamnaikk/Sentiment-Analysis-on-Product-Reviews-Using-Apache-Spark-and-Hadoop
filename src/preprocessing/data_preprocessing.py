from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, length, lit, when, expr
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
    count = df.count()
    logger.info(f"Loaded {count} records for {category}")
    
    # Add category column if it doesn't exist
    if "category" not in df.columns:
        df = df.withColumn("category", lit(category))
    
    return df

def preprocess_data(df, category):
    """Preprocess the Amazon reviews data"""
    logger.info(f"Preprocessing {category} data...")
    
    # Count initial records
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count}")
    
    # Print schema to log for debugging
    logger.info(f"DataFrame schema for {category}:")
    df.printSchema()
    
    # Sample a few rows to understand the data structure
    logger.info(f"Sample rows from {category} (first 2 rows):")
    sample_rows = df.limit(2).collect()
    for row in sample_rows:
        logger.info(str(row))
    
    # Since we're working with product metadata (not reviews),
    # we need to adjust our processing logic
    
    # Identify key fields and handle missing or renamed columns
    # We'll create a consistent schema across all categories
    
    # Start with common columns
    selected_columns = []
    
    # Product ID (could be asin or parent_asin)
    if "parent_asin" in df.columns:
        selected_columns.append(col("parent_asin").alias("product_id"))
    elif "asin" in df.columns:
        selected_columns.append(col("asin").alias("product_id"))
    else:
        # If neither exists, create a dummy ID
        logger.warning(f"No product ID field found in {category}. Using a placeholder.")
        selected_columns.append(lit("unknown").alias("product_id"))
    
    # Product title
    if "title" in df.columns:
        selected_columns.append(col("title"))
    else:
        selected_columns.append(lit(None).alias("title"))
    
    # Product rating
    if "average_rating" in df.columns:
        selected_columns.append(col("average_rating").alias("rating"))
    elif "overall" in df.columns:
        selected_columns.append(col("overall").alias("rating"))
    else:
        selected_columns.append(lit(None).alias("rating"))
    
    # Number of ratings
    if "rating_number" in df.columns:
        selected_columns.append(col("rating_number"))
    else:
        selected_columns.append(lit(None).alias("rating_number"))
    
    # Price
    if "price" in df.columns:
        selected_columns.append(col("price"))
    else:
        selected_columns.append(lit(None).alias("price"))
    
    # Main category
    if "main_category" in df.columns:
        selected_columns.append(col("main_category"))
    else:
        selected_columns.append(lit(None).alias("main_category"))
    
    # Store
    if "store" in df.columns:
        selected_columns.append(col("store"))
    else:
        selected_columns.append(lit(None).alias("store"))
    
    # Description - convert array to string if needed
    if "description" in df.columns:
        if "array" in df.schema["description"].dataType.simpleString():
            # Convert array to string
            selected_columns.append(expr("array_join(description, ' ')").alias("description"))
        else:
            selected_columns.append(col("description"))
    else:
        selected_columns.append(lit(None).alias("description"))
    
    # Features - convert array to string if needed
    if "features" in df.columns:
        if "array" in df.schema["features"].dataType.simpleString():
            # Convert array to string
            selected_columns.append(expr("array_join(features, ' ')").alias("features"))
        else:
            selected_columns.append(col("features"))
    else:
        selected_columns.append(lit(None).alias("features"))
    
    # Categories - convert array to string if needed
    if "categories" in df.columns:
        if "array" in df.schema["categories"].dataType.simpleString():
            # Convert array to string
            selected_columns.append(expr("array_join(categories, ' ')").alias("categories"))
        else:
            selected_columns.append(col("categories"))
    else:
        selected_columns.append(lit(None).alias("categories"))

    # Add dataset category
    selected_columns.append(col("category"))
    
    # Select columns and handle nulls
    df = df.select(*selected_columns)
    
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