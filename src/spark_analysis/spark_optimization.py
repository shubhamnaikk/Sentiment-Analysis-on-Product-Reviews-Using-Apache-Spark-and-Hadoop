#!/usr/bin/env python3
"""
Spark job optimization techniques and performance benchmarking.
This script demonstrates various optimization techniques for Spark jobs.
"""

import argparse
import logging
import sys
import os
import time
from pyspark.sql.functions import col, length, explode, split, desc
from pyspark.sql.window import Window

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.spark_analysis.spark_utils import (
    create_spark_session,
    load_amazon_reviews,
    preprocess_reviews_dataframe,
    save_results
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def benchmark_operation(operation_name, operation_func, *args, **kwargs):
    """
    Benchmark a Spark operation.
    
    Args:
        operation_name (str): Name of the operation
        operation_func (function): Function to execute
        *args, **kwargs: Arguments to pass to the function
        
    Returns:
        tuple: (result of the operation, execution time in seconds)
    """
    logger.info(f"Starting benchmark for: {operation_name}")
    start_time = time.time()
    
    result = operation_func(*args, **kwargs)
    
    # Force evaluation if result is a DataFrame
    if hasattr(result, "count"):
        count = result.count()
        logger.info(f"Operation returned {count} records")
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    logger.info(f"Benchmark for '{operation_name}' completed in {execution_time:.2f} seconds")
    
    return result, execution_time

def compare_optimization_techniques(df, output_dir):
    """
    Compare different optimization techniques on the same operations.
    
    Args:
        df (DataFrame): DataFrame to optimize
        output_dir (str): Directory to save results
        
    Returns:
        DataFrame: Benchmark results
    """
    logger.info("Starting optimization techniques comparison")
    
    spark = df.sparkSession
    benchmark_results = []
    
    # 1. Default operation - No optimization
    def default_word_count():
        # Split review text into words and count frequency
        words_df = df.select(
            explode(split(col("reviewText"), " ")).alias("word")
        )
        return words_df.groupBy("word").count().orderBy(desc("count")).limit(100)
    
    _, default_time = benchmark_operation("Default word count", default_word_count)
    benchmark_results.append(("default_word_count", default_time))
    
    # 2. Cached DataFrame
    df.cache()
    df.count()  # Force caching
    
    def cached_word_count():
        words_df = df.select(
            explode(split(col("reviewText"), " ")).alias("word")
        )
        return words_df.groupBy("word").count().orderBy(desc("count")).limit(100)
    
    _, cached_time = benchmark_operation("Cached DataFrame word count", cached_word_count)
    benchmark_results.append(("cached_word_count", cached_time))
    
    # 3. Optimized partitioning
    partition_count = max(8, spark.sparkContext.defaultParallelism * 2)
    df_repartitioned = df.repartition(partition_count)
    df_repartitioned.cache()
    df_repartitioned.count()  # Force caching
    
    def repartitioned_word_count():
        words_df = df_repartitioned.select(
            explode(split(col("reviewText"), " ")).alias("word")
        )
        return words_df.groupBy("word").count().orderBy(desc("count")).limit(100)
    
    _, repartitioned_time = benchmark_operation("Repartitioned DataFrame word count", repartitioned_word_count)
    benchmark_results.append(("repartitioned_word_count", repartitioned_time))
    
    # 4. Broadcast join optimization
    def standard_join():
        # Create a small DataFrame for categories
        categories = df.select("category").distinct()
        # Join with main dataset
        return df.join(categories, "category")
    
    _, standard_join_time = benchmark_operation("Standard join", standard_join)
    benchmark_results.append(("standard_join", standard_join_time))
    
    def broadcast_join():
        from pyspark.sql.functions import broadcast
        # Create a small DataFrame for categories
        categories = df.select("category").distinct()
        # Use broadcast join
        return df.join(broadcast(categories), "category")
    
    _, broadcast_join_time = benchmark_operation("Broadcast join", broadcast_join)
    benchmark_results.append(("broadcast_join", broadcast_join_time))
    
    # 5. SQL vs DataFrame API
    df.createOrReplaceTempView("reviews")
    
    def sql_api():
        return spark.sql("""
            SELECT overall, AVG(length(reviewText)) as avg_length
            FROM reviews
            GROUP BY overall
            ORDER BY overall
        """)
    
    _, sql_time = benchmark_operation("SQL API", sql_api)
    benchmark_results.append(("sql_api", sql_time))
    
    def dataframe_api():
        return df.groupBy("overall") \
            .agg({"reviewText": "avg"}) \
            .orderBy("overall")
    
    _, dataframe_time = benchmark_operation("DataFrame API", dataframe_api)
    benchmark_results.append(("dataframe_api", dataframe_time))
    
    # Create a DataFrame with benchmark results
    benchmark_df = spark.createDataFrame(
        [(name, time) for name, time in benchmark_results],
        ["operation", "execution_time_seconds"]
    )
    
    # Save benchmark results
    save_results(benchmark_df, f"{output_dir}/optimization_benchmark_results", "csv")
    
    logger.info("Optimization techniques comparison completed")
    
    return benchmark_df

def optimize_configuration(spark, df, output_dir):
    """
    Test different Spark configurations and measure their impact.
    
    Args:
        spark (SparkSession): Spark session
        df (DataFrame): DataFrame to use for testing
        output_dir (str): Directory to save results
        
    Returns:
        DataFrame: Configuration test results
    """
    logger.info("Starting Spark configuration optimization tests")
    
    # Define a test operation that will be used for all config tests
    def test_operation():
        result = df.groupBy("overall") \
            .agg({"reviewText": "count", "review_length": "avg"}) \
            .orderBy("overall")
        return result.count()
    
    # Baseline with current configuration
    current_shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    current_executor_memory = spark.conf.get("spark.executor.memory", "1g")
    
    logger.info(f"Current configuration: shuffle_partitions={current_shuffle_partitions}, executor_memory={current_executor_memory}")
    
    # Run baseline test
    _, baseline_time = benchmark_operation("Baseline configuration", test_operation)
    
    config_results = [("baseline", int(current_shuffle_partitions), current_executor_memory, baseline_time)]
    
    # Test different shuffle partition counts
    partition_tests = [5, 10, 20, 50]
    
    for partitions in partition_tests:
        # Skip if it's the same as current
        if str(partitions) == current_shuffle_partitions:
            continue
            
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        logger.info(f"Testing with shuffle_partitions={partitions}")
        
        _, test_time = benchmark_operation(f"Shuffle partitions={partitions}", test_operation)
        config_results.append(("shuffle_partitions", partitions, current_executor_memory, test_time))
    
    # Reset to best shuffle partition configuration
    best_shuffle_result = min(
        [r for r in config_results if r[0] == "shuffle_partitions"] or [(None, None, None, float('inf'))],
        key=lambda x: x[3]
    )
    
    if best_shuffle_result[0] is not None:
        best_partitions = best_shuffle_result[1]
        logger.info(f"Best shuffle partition count: {best_partitions}")
        spark.conf.set("spark.sql.shuffle.partitions", best_partitions)
    else:
        # Reset to original
        spark.conf.set("spark.sql.shuffle.partitions", current_shuffle_partitions)
    
    # Create a DataFrame with configuration test results
    config_df = spark.createDataFrame(
        [(test, partitions, memory, time) for test, partitions, memory, time in config_results],
        ["test_name", "shuffle_partitions", "executor_memory", "execution_time_seconds"]
    )
    
    # Save configuration test results
    save_results(config_df, f"{output_dir}/configuration_test_results", "csv")
    
    # Reset to original configuration
    spark.conf.set("spark.sql.shuffle.partitions", current_shuffle_partitions)
    
    logger.info("Spark configuration optimization tests completed")
    
    return config_df

def main():
    """Main function to run the Spark optimization benchmarks."""
    parser = argparse.ArgumentParser(description="Optimize and benchmark Spark jobs for Amazon reviews")
    parser.add_argument("--input", required=True, help="Input data path (HDFS or local)")
    parser.add_argument("--output", required=True, help="Output directory for results")
    parser.add_argument("--format", default="parquet", help="Input data format (parquet, json, csv)")
    
    args = parser.parse_args()
    
    # Create Spark session with custom configs for optimization
    spark = create_spark_session(
        "Amazon Reviews Spark Optimization",
        configs={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true"
        }
    )
    
    try:
        # Load data
        reviews_df = load_amazon_reviews(spark, args.input, args.format)
        
        # Preprocess the data
        processed_df = preprocess_reviews_dataframe(reviews_df)
        
        # Create output directories
        os.makedirs(args.output, exist_ok=True)
        
        # Run optimization technique comparison
        benchmark_df = compare_optimization_techniques(processed_df, args.output)
        
        # Test different Spark configurations
        config_df = optimize_configuration(spark, processed_df, args.output)
        
        # Provide recommendations based on benchmarks
        best_operation = min(benchmark_df.collect(), key=lambda x: x["execution_time_seconds"])
        
        logger.info(f"Optimization complete. Best operation: {best_operation['operation']} "
                   f"with execution time: {best_operation['execution_time_seconds']:.2f} seconds")
        
        # Create a recommendations document
        spark_recommendations = spark.createDataFrame([{
            "recommendation_type": "Best optimization technique",
            "recommendation": best_operation["operation"],
            "execution_time": best_operation["execution_time_seconds"],
            "details": "This technique provided the fastest execution time in benchmarks."
        }])
        
        # Save recommendations
        save_results(spark_recommendations, f"{output_dir}/optimization_recommendations", "csv")
        
    except Exception as e:
        logger.error(f"Error in Spark optimization: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # Stop the Spark session
        spark.stop()
        
if __name__ == "__main__":
    main()