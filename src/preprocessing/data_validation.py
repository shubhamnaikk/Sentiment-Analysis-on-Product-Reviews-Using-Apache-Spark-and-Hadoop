from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, avg, min, max, stddev
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/data_validation.log"),
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
        .appName("Amazon Reviews Data Validation") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    logger.info(f"Created Spark session: {spark.version}")
    return spark

def load_processed_data(spark, category):
    """Load processed data from HDFS"""
    hdfs_path = f"/user/amazon_reviews/processed/{category}_processed"
    logger.info(f"Loading processed data from {hdfs_path}")
    
    try:
        df = spark.read.parquet(hdfs_path)
        record_count = df.count()
        logger.info(f"Loaded {record_count} records for {category}")
        return df
    except Exception as e:
        logger.error(f"Error loading data for {category}: {str(e)}")
        return None

def validate_data(df, category):
    """Perform validation checks on the processed data"""
    if df is None:
        logger.error(f"No data to validate for {category}")
        return False
    
    logger.info(f"Validating {category} dataset...")
    
    # Count total records
    total_count = df.count()
    logger.info(f"Total records: {total_count}")
    
    # Check for missing values
    logger.info("Checking for missing values...")
    missing_counts = {}
    for column in df.columns:
        missing_count = df.filter(col(column).isNull()).count()
        missing_percentage = (missing_count / total_count) * 100
        missing_counts[column] = (missing_count, missing_percentage)
        logger.info(f"  - {column}: {missing_count} missing ({missing_percentage:.2f}%)")
    
    # Check rating distribution
    logger.info("Checking rating distribution...")
    rating_dist = df.groupBy("rating").count().orderBy("rating")
    rating_dist.show()
    
    # Get rating statistics
    rating_stats = df.select(
        min("rating").alias("min_rating"),
        max("rating").alias("max_rating"),
        avg("rating").alias("avg_rating"),
        stddev("rating").alias("stddev_rating")
    ).collect()[0]
    
    logger.info(f"Rating statistics: Min={rating_stats['min_rating']}, Max={rating_stats['max_rating']}, Avg={rating_stats['avg_rating']:.2f}, StdDev={rating_stats['stddev_rating']:.2f}")
    
    # Check review length distribution
    if "review_length" in df.columns:
        logger.info("Checking review length distribution...")
        length_stats = df.select(
            min("review_length").alias("min_length"),
            max("review_length").alias("max_length"),
            avg("review_length").alias("avg_length"),
            stddev("review_length").alias("stddev_length")
        ).collect()[0]
        
        logger.info(f"Review length statistics: Min={length_stats['min_length']}, Max={length_stats['max_length']}, Avg={length_stats['avg_length']:.2f}, StdDev={length_stats['stddev_length']:.2f}")
    
    # Check category distribution
    if "category" in df.columns:
        logger.info("Checking category distribution...")
        df.groupBy("category").count().show()
    
    # Check for any anomalies or outliers
    logger.info("Checking for outliers in reviews...")
    outlier_threshold = 10000  # Example threshold for review length
    if "review_length" in df.columns:
        outliers = df.filter(col("review_length") > outlier_threshold).count()
        logger.info(f"Reviews with length > {outlier_threshold}: {outliers} ({(outliers/total_count)*100:.2f}%)")
    
    # Generate validation report
    validation_results = {
        "total_records": total_count,
        "missing_values": missing_counts,
        "rating_stats": rating_stats,
        "validation_status": "Passed" if total_count > 0 else "Failed"
    }
    
    logger.info(f"Validation completed for {category}: {validation_results['validation_status']}")
    return validation_results

def validate_all_categories(spark):
    """Validate all processed datasets"""
    categories = ["electronics", "toys_games", "cell_phones", "all_reviews"]
    validation_reports = {}
    
    for category in categories:
        df = load_processed_data(spark, category)
        if df is not None:
            validation_report = validate_data(df, category)
            validation_reports[category] = validation_report
    
    # Log overall validation status
    success_count = sum(1 for report in validation_reports.values() if report["validation_status"] == "Passed")
    logger.info(f"Validation completed for {len(validation_reports)} categories. Success: {success_count}, Failed: {len(validation_reports) - success_count}")
    
    return validation_reports

if __name__ == "__main__":
    try:
        logger.info("Starting data validation process...")
        spark = create_spark_session()
        validation_reports = validate_all_categories(spark)
        spark.stop()
        logger.info("Data validation process completed")
    except Exception as e:
        logger.error(f"Error in validation process: {str(e)}")