import os
import subprocess
from pyspark.sql import SparkSession

# Define expected raw file names and HDFS paths
raw_files = [
    "Electronics.json.gz",
    "Toys_Games.json.gz",
    "Cell_Phones.json.gz"
]

hdfs_raw_dir = "/user/amazon_reviews/raw"
hdfs_processed_dir = "/user/amazon_reviews/processed"
processed_categories = [
    "all_reviews_processed",
    "electronics_processed",
    "toys_games_processed",
    "cell_phones_processed"
]

def check_local_files():
    print("\n✅ Checking local raw files...")
    missing = []
    for file in raw_files:
        if not os.path.isfile(f"data/raw/{file}"):
            missing.append(file)
    if missing:
        print("❌ Missing raw files:", missing)
    else:
        print("✅ All raw files found.")

def check_hdfs_files():
    print("\n✅ Checking HDFS raw upload...")
    try:
        output = subprocess.check_output(["hdfs", "dfs", "-ls", hdfs_raw_dir]).decode()
        found = [f for f in raw_files if f in output]
        missing = [f for f in raw_files if f not in output]
        if missing:
            print("❌ Missing files in HDFS raw folder:", missing)
        else:
            print("✅ All raw files found in HDFS.")
    except subprocess.CalledProcessError as e:
        print("❌ Error accessing HDFS:", e.output.decode())

def check_parquet_outputs(spark):
    print("\n✅ Checking processed Parquet outputs...")
    for cat in processed_categories:
        path = f"hdfs://localhost:9000{hdfs_processed_dir}/{cat}"
        try:
            df = spark.read.parquet(path)
            count = df.count()
            print(f"✅ {cat}: {count} records found.")
        except Exception as e:
            print(f"❌ Failed to read {cat}: {str(e)}")

def main():
    check_local_files()
    check_hdfs_files()

    spark = SparkSession.builder \
        .appName("Verify Pipeline") \
        .getOrCreate()
    
    check_parquet_outputs(spark)
    spark.stop()

if __name__ == "__main__":
    main()
