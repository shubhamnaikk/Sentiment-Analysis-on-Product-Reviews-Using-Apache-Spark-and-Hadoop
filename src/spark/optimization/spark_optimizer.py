# src/spark/optimization/spark_optimizer.py
from pyspark.sql import SparkSession
import logging
import time

logger = logging.getLogger(__name__)

def optimize_spark_session(spark):
    """
    Apply optimizations to a Spark session.
    
    Args:
        spark (SparkSession): Spark session to optimize
        
    Returns:
        SparkSession: Optimized Spark session
    """
    # SQL optimizations
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
    
    # Memory optimizations
    spark.conf.set("spark.memory.fraction", "0.8")
    spark.conf.set("spark.memory.storageFraction", "0.3")
    
    # Shuffle optimizations
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.default.parallelism", "20")
    
    # I/O optimizations
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    # Dynamic allocation
    spark.conf.set("spark.dynamicAllocation.enabled", "true")
    spark.conf.set("spark.dynamicAllocation.minExecutors", "1")
    spark.conf.set("spark.dynamicAllocation.maxExecutors", "4")
    
    # Caching optimization
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
    
    # Network timeout
    spark.conf.set("spark.network.timeout", "800s")
    spark.conf.set("spark.executor.heartbeatInterval", "60s")
    
    logger.info("Applied all optimizations to Spark session")
    
    return spark
# src/spark/optimization/spark_optimizer.py (continued)
def measure_performance(func, *args, **kwargs):
    """
    Measure the performance of a function.
    
    Args:
        func: Function to measure
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        tuple: (result, execution_time)
    """
    start_time = time.time()
    result = func(*args, **kwargs)
    execution_time = time.time() - start_time
    
    return result, execution_time

def log_metrics(spark, job_name):
    """
    Log Spark metrics for a job.
    
    Args:
        spark (SparkSession): Spark session
        job_name (str): Name of the job
    """
    metrics = spark.sparkContext.statusTracker.getExecutorInfos()
    logger.info(f"Metrics for job {job_name}:")
    
    for metric in metrics:
        logger.info(f"  Executor {metric.id}: {metric.hostPort}")
        logger.info(f"    Total cores: {metric.totalCores}")
        logger.info(f"    Max memory: {metric.maxMemory / (1024 * 1024)} MB")
    
    # Log application information
    app_id = spark.sparkContext.applicationId
    app_info = spark.sparkContext.statusTracker.getApplicationInfo()
    logger.info(f"Application {app_id} info:")
    logger.info(f"  Start time: {app_info.startTime}")
    logger.info(f"  Duration: {(time.time() * 1000 - app_info.startTime) / 1000} seconds")

def optimize_dataframe_operations(df):
    """
    Apply DataFrame operation optimizations.
    
    Args:
        df (DataFrame): DataFrame to optimize
        
    Returns:
        DataFrame: Optimized DataFrame
    """
    # Persist the DataFrame with the right storage level
    from pyspark.storagelevel import StorageLevel
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    
    # Return the optimized DataFrame
    return df

def compare_optimization_strategies(spark, df, analysis_func, strategies, *args, **kwargs):
    """
    Compare different optimization strategies for an analysis function.
    
    Args:
        spark (SparkSession): Spark session
        df (DataFrame): Input DataFrame
        analysis_func: Analysis function to test
        strategies (list): List of optimization strategies to compare
        *args: Additional arguments for the analysis function
        **kwargs: Additional keyword arguments for the analysis function
        
    Returns:
        dict: Results of each strategy with execution times
    """
    results = {}
    
    # Test each strategy
    for strategy_name, strategy_func in strategies.items():
        logger.info(f"Testing optimization strategy: {strategy_name}")
        
        # Apply the strategy
        optimized_df = strategy_func(df)
        
        # Run the analysis function and measure performance
        result, execution_time = measure_performance(analysis_func, spark, optimized_df, *args, **kwargs)
        
        # Store the result
        results[strategy_name] = {
            "result": result,
            "execution_time": execution_time
        }
        
        logger.info(f"Strategy {strategy_name} completed in {execution_time:.2f} seconds")
        
        # Unpersist the optimized DataFrame if it was persisted
        try:
            optimized_df.unpersist()
        except:
            pass
    
    return results