# src/spark/utils/spark_session.py
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def create_spark_session(app_name="Amazon Reviews Analysis", optimize=True):
    """
    Create and return an optimized Spark session.
    
    Args:
        app_name (str): Name of the Spark application
        optimize (bool): Whether to apply optimization configurations
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Start with basic builder
    builder = SparkSession.builder.appName(app_name)
    
    # Apply optimizations if requested
    if optimize:
        builder = builder \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.sql.adaptive.skewedJoin.enabled", "true") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "1") \
            .config("spark.dynamicAllocation.maxExecutors", "4") \
            .config("spark.sql.files.maxPartitionBytes", "134217728") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.default.parallelism", "20") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.network.timeout", "800s")
    
    # Create and return session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Log session info
    logger.info(f"Created Spark session v{spark.version} with app name: {app_name}")
    
    return spark

def optimize_existing_session(spark):
    """
    Apply optimizations to an existing Spark session.
    
    Args:
        spark (SparkSession): Existing Spark session
    
    Returns:
        SparkSession: Optimized Spark session
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
    spark.conf.set("spark.memory.fraction", "0.8")
    spark.conf.set("spark.memory.storageFraction", "0.3")
    
    logger.info("Applied optimizations to existing Spark session")
    
    return spark