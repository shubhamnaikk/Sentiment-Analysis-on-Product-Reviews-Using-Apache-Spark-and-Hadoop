# src/spark/utils/visualization_helper.py
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import logging

logger = logging.getLogger(__name__)

def spark_df_to_pandas(spark_df):
    """
    Convert a Spark DataFrame to a Pandas DataFrame.
    
    Args:
        spark_df: Spark DataFrame
        
    Returns:
        pandas.DataFrame: Pandas DataFrame
    """
    return spark_df.toPandas()

def save_trend_visualizations(trend_results, output_dir="data/spark_results/visualizations/trends"):
    """
    Create and save visualizations for trend analysis results.
    
    Args:
        trend_results (dict): Dictionary of trend DataFrames
        output_dir (str): Output directory
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Monthly trends
    if "monthly" in trend_results:
        try:
            # Convert to pandas
            monthly_df = spark_df_to_pandas(trend_results["monthly"])
            
            # Create date column
            monthly_df['date'] = pd.to_datetime(monthly_df.apply(
                lambda row: f"{int(row['year'])}-{int(row['month'])}-01", axis=1
            ))
            monthly_df = monthly_df.sort_values('date')
            
            # Plot monthly rating trends
            plt.figure(figsize=(12, 6))
            plt.plot(monthly_df['date'], monthly_df['avg_rating'], marker='o', linestyle='-', markersize=3)
            plt.title('Average Rating by Month')
            plt.xlabel('Date')
            plt.ylabel('Average Rating')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(f"{output_dir}/monthly_rating_trend.png", dpi=300)
            plt.close()
            
            # Plot monthly review count
            plt.figure(figsize=(12, 6))
            plt.plot(monthly_df['date'], monthly_df['review_count'], marker='o', linestyle='-', markersize=3)
            plt.title('Review Count by Month')
            plt.xlabel('Date')
            plt.ylabel('Review Count')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(f"{output_dir}/monthly_review_count.png", dpi=300)
            plt.close()
            
            logger.info(f"Saved monthly trend visualizations to {output_dir}")
        except Exception as e:
            logger.error(f"Error creating monthly trend visualizations: {str(e)}", exc_info=True)
    
    # Yearly trends
    if "yearly" in trend_results:
        try:
            # Convert to pandas
            yearly_df = spark_df_to_pandas(trend_results["yearly"])
            yearly_df = yearly_df.sort_values('year')
            
            # Plot yearly rating trends
            plt.figure(figsize=(10, 6))
            plt.bar(yearly_df['year'].astype(str), yearly_df['avg_rating'], color='skyblue')
            plt.title('Average Rating by Year')
            plt.xlabel('Year')
            plt.ylabel('Average Rating')
            plt.grid(True, alpha=0.3, axis='y')
            plt.tight_layout()
            plt.savefig(f"{output_dir}/yearly_rating_trend.png", dpi=300)
            plt.close()
            
            # Plot yearly review count
            plt.figure(figsize=(10, 6))
            plt.bar(yearly_df['year'].astype(str), yearly_df['review_count'], color='lightgreen')
            plt.title('Review Count by Year')
            plt.xlabel('Year')
            plt.ylabel('Review Count')
            plt.grid(True, alpha=0.3, axis='y')
            plt.tight_layout()
            plt.savefig(f"{output_dir}/yearly_review_count.png", dpi=300)
            plt.close()
            
            logger.info(f"Saved yearly trend visualizations to {output_dir}")
        except Exception as e:
            logger.error(f"Error creating yearly trend visualizations: {str(e)}", exc_info=True)
    
    # Rolling trends
    if "rolling" in trend_results:
        try:
            # Convert to pandas
            rolling_df = spark_df_to_pandas(trend_results["rolling"])
            
            # Create date column
            rolling_df['date'] = pd.to_datetime(rolling_df.apply(
                lambda row: f"{int(row['year'])}-{int(row['month'])}-01", axis=1
            ))
            rolling_df = rolling_df.sort_values('date')
            
            # Plot rolling average trends
            plt.figure(figsize=(12, 6))
            plt.plot(rolling_df['date'], rolling_df['avg_rating'], marker='o', linestyle='-', label='Monthly Avg', markersize=3)
            plt.plot(rolling_df['date'], rolling_df['rolling_avg'], linestyle='-', color='red', linewidth=2, label='3-Month Rolling Avg')
            plt.title('Monthly vs. Rolling Average Rating')
            plt.xlabel('Date')
            plt.ylabel('Average Rating')
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(f"{output_dir}/rolling_rating_trend.png", dpi=300)
            plt.close()
            
            logger.info(f"Saved rolling trend visualizations to {output_dir}")
        except Exception as e:
            logger.error(f"Error creating rolling trend visualizations: {str(e)}", exc_info=True)

def save_bias_visualizations(bias_results, output_dir="data/spark_results/visualizations/bias"):
    """
    Create and save visualizations for bias analysis results.
    
    Args:
        bias_results (dict): Dictionary of bias analysis results
        output_dir (str): Output directory
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Bucket stats visualization
    if "bucket_stats" in bias_results:
        try:
            # Convert to pandas
            bucket_df = spark_df_to_pandas(bias_results["bucket_stats"])
            
            # Order the length buckets
            bucket_order = ["Very Short", "Short", "Medium", "Long", "Very Long"]
            bucket_df['length_bucket'] = pd.Categorical(bucket_df['length_bucket'], categories=bucket_order, ordered=True)
            bucket_df = bucket_df.sort_values('length_bucket')
            
            # Plot average rating by review length bucket
            plt.figure(figsize=(10, 6))
            bar = plt.bar(bucket_df['length_bucket'], bucket_df['avg_rating'], color=sns.color_palette("Blues_d", len(bucket_df)))
            plt.title('Average Rating by Review Length')
            plt.xlabel('Review Length')
            plt.ylabel('Average Rating')
            plt.grid(True, alpha=0.3, axis='y')
            
            # Add count labels
            for rect, count in zip(bar, bucket_df['count']):
                height = rect.get_height()
                plt.text(rect.get_x() + rect.get_width()/2., height + 0.05,
                        f'n={count}', ha='center', va='bottom', fontsize=10)
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/rating_by_length_bucket.png", dpi=300)
            plt.close()
            
            logger.info(f"Saved bias bucket visualizations to {output_dir}")
        except Exception as e:
            logger.error(f"Error creating bias bucket visualizations: {str(e)}", exc_info=True)
    
    # Rating pivot visualization
    if "rating_pivot" in bias_results:
        try:
            # Convert to pandas
            pivot_df = spark_df_to_pandas(bias_results["rating_pivot"])
            pivot_df = pivot_df.sort_values('rating_rounded')
            
            # Fill NaN values with 0
            pivot_df = pivot_df.fillna(0)
            
            # Select only columns for the length buckets
            bucket_columns = [col for col in pivot_df.columns if col != 'rating_rounded']
            
            # Plot stacked bar chart of review counts by rating and length
            plt.figure(figsize=(12, 8))
            
            bottom = np.zeros(len(pivot_df))
            for i, col in enumerate(bucket_columns):
                plt.bar(pivot_df['rating_rounded'], pivot_df[col], bottom=bottom, label=col)
                bottom += pivot_df[col].fillna(0).values
            
            plt.title('Number of Reviews by Rating and Length')
            plt.xlabel('Rating')
            plt.ylabel('Count')
            plt.xticks(pivot_df['rating_rounded'])
            plt.legend(title='Review Length')
            plt.grid(True, alpha=0.3, axis='y')
            plt.tight_layout()
            plt.savefig(f"{output_dir}/rating_by_length_stacked.png", dpi=300)
            plt.close()
            
            logger.info(f"Saved bias pivot visualizations to {output_dir}")
        except Exception as e:
            logger.error(f"Error creating bias pivot visualizations: {str(e)}", exc_info=True)
    
    # Create correlation visualization
    if "correlation" in bias_results:
        try:
            # Create a simple visualization of the correlation value
            plt.figure(figsize=(8, 2))
            corr = bias_results["correlation"]
            plt.axhline(y=0, color='k', linestyle='-', alpha=0.3)
            plt.scatter([corr], [0], s=100, color='blue')
            plt.xlim(-1, 1)
            plt.title(f'Correlation between Review Length and Rating: {corr:.4f}')
            plt.xlabel('Correlation Coefficient')
            plt.yticks([])
            
            # Add colored regions for interpretation
            plt.axvspan(-1, -0.5, alpha=0.2, color='red')
            plt.axvspan(-0.5, -0.3, alpha=0.2, color='orange')
            plt.axvspan(-0.3, 0.3, alpha=0.2, color='gray')
            plt.axvspan(0.3, 0.5, alpha=0.2, color='lightgreen')
            plt.axvspan(0.5, 1, alpha=0.2, color='green')
            
            # Add labels
            plt.text(-0.75, 0, 'Strong Negative', ha='center', va='bottom', fontsize=8)
            plt.text(-0.4, 0, 'Moderate Negative', ha='center', va='bottom', fontsize=8)
            plt.text(0, 0, 'Weak/No Correlation', ha='center', va='bottom', fontsize=8)
            plt.text(0.4, 0, 'Moderate Positive', ha='center', va='bottom', fontsize=8)
            plt.text(0.75, 0, 'Strong Positive', ha='center', va='bottom', fontsize=8)
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/length_rating_correlation.png", dpi=300)
            plt.close()
            
            logger.info(f"Saved correlation visualization to {output_dir}")
        except Exception as e:
            logger.error(f"Error creating correlation visualization: {str(e)}", exc_info=True)

def save_stats_visualizations(stats_results, output_dir="data/spark_results/visualizations/stats"):
    """
    Create and save visualizations for statistical analysis results.
    
    Args:
        stats_results (dict): Dictionary of statistical analysis results
        output_dir (str): Output directory
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Histogram visualization
    if "histogram" in stats_results:
        try:
            # Convert to pandas
            hist_df = spark_df_to_pandas(stats_results["histogram"])
            hist_df = hist_df.sort_values('rating_bucket')
            
            # Plot histogram of ratings
            plt.figure(figsize=(12, 6))
            plt.bar(hist_df['rating_bucket'], hist_df['count'], width=0.4, color='skyblue')
            plt.title('Distribution of Ratings')
            plt.xlabel('Rating')
            plt.ylabel('Count')
            plt.grid(True, alpha=0.3, axis='y')
            plt.xticks(hist_df['rating_bucket'])
            
            # Add percentage labels
            for i, row in hist_df.iterrows():
                plt.text(row['rating_bucket'], row['count'] + 5, 
                        f"{row['percentage']:.1f}%", 
                        ha='center', va='bottom', fontsize=9)
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/ratings_histogram.png", dpi=300)
            plt.close()
            
            logger.info(f"Saved histogram visualization to {output_dir}")
        except Exception as e:
            logger.error(f"Error creating histogram visualization: {str(e)}", exc_info=True)
    
    # Star distribution visualization
    if "star_distribution" in stats_results:
        try:
            # Convert to pandas
            star_df = spark_df_to_pandas(stats_results["star_distribution"])
            star_df = star_df.sort_values('star_rating')
            
            # Plot star distribution
            plt.figure(figsize=(10, 6))
            bars = plt.bar(star_df['star_rating'], star_df['count'], color=sns.color_palette("YlOrRd", len(star_df)))
            plt.title('Distribution of Star Ratings')
            plt.xlabel('Star Rating')
            plt.ylabel('Count')
            plt.grid(True, alpha=0.3, axis='y')
            plt.xticks(star_df['star_rating'])
            
            # Add percentage labels
            for bar, percentage in zip(bars, star_df['percentage']):
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f"{percentage:.1f}%", ha='center', va='bottom', fontsize=10)
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/star_distribution.png", dpi=300)
            plt.close()
            
            logger.info(f"Saved star distribution visualization to {output_dir}")
        except Exception as e:
            logger.error(f"Error creating star distribution visualization: {str(e)}", exc_info=True)
    
    # Basic stats visualization
    if "basic_stats" in stats_results:
        try:
            # Convert to pandas
            stats_df = spark_df_to_pandas(stats_results["basic_stats"])
            
            # Create a box plot representation of the statistics
            plt.figure(figsize=(8, 6))
            
            # Extract values
            min_val = stats_df['min'].iloc[0]
            q1_val = stats_df['q1'].iloc[0]
            median_val = stats_df['median'].iloc[0]
            q3_val = stats_df['q3'].iloc[0]
            max_val = stats_df['max'].iloc[0]
            mean_val = stats_df['mean'].iloc[0]
            
            # Create box plot with custom values
            box_data = [[min_val, q1_val, median_val, q3_val, max_val]]
            plt.boxplot(box_data, vert=False, patch_artist=True, 
                       boxprops=dict(facecolor='lightblue', color='blue'),
                       medianprops=dict(color='red', linewidth=2))
            
            # Add mean marker
            plt.plot([mean_val], [1], 'go', ms=8, label='Mean')
            
            plt.title('Rating Distribution Statistics')
            plt.xlabel('Rating')
            plt.yticks([1], [''])
            plt.grid(True, alpha=0.3, axis='x')
            
            # Add text annotations
            plt.text(min_val, 1.15, f"Min: {min_val:.2f}", ha='center', va='bottom', fontsize=10)
            plt.text(q1_val, 1.15, f"Q1: {q1_val:.2f}", ha='center', va='bottom', fontsize=10)
            plt.text(median_val, 1.15, f"Median: {median_val:.2f}", ha='center', va='bottom', fontsize=10)
            plt.text(q3_val, 1.15, f"Q3: {q3_val:.2f}", ha='center', va='bottom', fontsize=10)
            plt.text(max_val, 1.15, f"Max: {max_val:.2f}", ha='center', va='bottom', fontsize=10)
            plt.text(mean_val, 0.85, f"Mean: {mean_val:.2f}", ha='center', va='top', fontsize=10, color='green')
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/rating_statistics.png", dpi=300)
            plt.close()
            
            logger.info(f"Saved basic statistics visualization to {output_dir}")
        except Exception as e:
            logger.error(f"Error creating basic statistics visualization: {str(e)}", exc_info=True)