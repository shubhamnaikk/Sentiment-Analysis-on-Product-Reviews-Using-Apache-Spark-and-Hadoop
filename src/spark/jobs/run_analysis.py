import logging
import sys
import os
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import create_spark_session
from utils.data_loader import load_mapreduce_data, prepare_data_for_analysis
from analysis.trend_analysis import analyze_rating_trends, save_trend_analysis
from analysis.bias_detection import analyze_review_bias, save_bias_analysis
from analysis.stats_analysis import analyze_ratings_distribution, save_stats_analysis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/spark_analysis.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_analysis():
    """
    Run the complete Spark analysis pipeline.
    """
    start_time = time.time()
    logger.info("Starting Spark analysis pipeline...")
    
    try:
        # Create Spark session
        spark = create_spark_session(app_name="Amazon Reviews Analysis")
        
        # Load and prepare data
        logger.info("Loading and preparing data...")
        data_dict = prepare_data_for_analysis(spark)
        
        # Create output directory
        os.makedirs("data/spark_results", exist_ok=True)
        
        # Process each category
        for category, df in data_dict.items():
            logger.info(f"Processing {category} category...")
            
            # Create category output directory
            os.makedirs(f"data/spark_results/{category}", exist_ok=True)
            os.makedirs(f"data/spark_results/{category}/trends", exist_ok=True)
            os.makedirs(f"data/spark_results/{category}/bias", exist_ok=True)
            os.makedirs(f"data/spark_results/{category}/stats", exist_ok=True)
            
            # Run trend analysis
            logger.info(f"Running trend analysis for {category}...")
            try:
                trend_results = analyze_rating_trends(spark, df, date_col="timestamp", rating_col="rating")
                if trend_results:
                    save_trend_analysis(trend_results, f"data/spark_results/{category}/trends")
                    logger.info(f"Saved trend analysis for {category}")
            except Exception as e:
                logger.error(f"Error in trend analysis for {category}: {str(e)}")
            
            # Run bias detection
            logger.info(f"Running bias detection for {category}...")
            try:
                bias_results = analyze_review_bias(spark, df, description_col="review_length", rating_col="rating")
                if bias_results:
                    save_bias_analysis(bias_results, f"data/spark_results/{category}/bias")
                    logger.info(f"Saved bias analysis for {category}")
            except Exception as e:
                logger.error(f"Error in bias detection for {category}: {str(e)}")
            
            # Run statistical analysis
            logger.info(f"Running statistical analysis for {category}...")
            try:
                stats_results = analyze_ratings_distribution(spark, df, rating_col="rating")
                if stats_results:
                    save_stats_analysis(stats_results, f"data/spark_results/{category}/stats")
                    logger.info(f"Saved statistical analysis for {category}")
            except Exception as e:
                logger.error(f"Error in statistical analysis for {category}: {str(e)}")
            
            logger.info(f"Completed analysis for {category}")
        
        # Process combined data from all categories
        logger.info("Processing combined data from all categories...")
        
        try:
            # Combine all DataFrames
            combined_df = None
            for df in data_dict.values():
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = combined_df.union(df)
            
            if combined_df:
                # Create output directory
                os.makedirs("data/spark_results/all_categories", exist_ok=True)
                os.makedirs("data/spark_results/all_categories/trends", exist_ok=True)
                os.makedirs("data/spark_results/all_categories/bias", exist_ok=True)
                os.makedirs("data/spark_results/all_categories/stats", exist_ok=True)
                
                # Run analyses on combined data
                logger.info("Running trend analysis on all categories...")
                trend_results = analyze_rating_trends(spark, combined_df, date_col="timestamp", rating_col="rating")
                if trend_results:
                    save_trend_analysis(trend_results, "data/spark_results/all_categories/trends")
                
                logger.info("Running bias detection on all categories...")
                bias_results = analyze_review_bias(spark, combined_df, description_col="review_length", rating_col="rating")
                if bias_results:
                    save_bias_analysis(bias_results, "data/spark_results/all_categories/bias")
                
                logger.info("Running statistical analysis on all categories...")
                stats_results = analyze_ratings_distribution(spark, combined_df, rating_col="rating")
                if stats_results:
                    save_stats_analysis(stats_results, "data/spark_results/all_categories/stats")
        except Exception as e:
            logger.error(f"Error processing combined data: {str(e)}")
        
        # Stop Spark session
        logger.info("Stopping Spark session...")
        spark.stop()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Spark analysis pipeline completed in {elapsed_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in Spark analysis pipeline: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(run_analysis())