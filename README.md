# Amazon Reviews Sentiment Analysis

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](#license)  [![Build Status](https://img.shields.io/badge/Status-Production%20Ready-green.svg)](#)

An end-to-end pipeline for performing sentiment analysis on Amazon product reviews using Apache Spark and Hadoop MapReduce. Leverage the power of distributed computing for both batch and streaming data processing, and generate rich insights into product performance, review bias, and rating trends.

---

## 🚀 Key Features

* **Hybrid Architecture**: Combines Hadoop MapReduce for large-scale batch computations and Apache Spark for fast, in-memory analytics.
* **Modular & Extensible**: Well-organized project structure with clear separation of preprocessing, MapReduce, and Spark analysis components.
* **Automated Workflows**: Shell scripts to configure Hadoop/Spark, run preprocessing, validation, MapReduce jobs, and Spark analytics.
* **Rich Insights**:

  * **Top Products & Categories**: Identify top-rated and most popular products/categories.
  * **Rating Distribution**: Detailed statistical summaries (mean, median, quartiles).
  * **Trend Analysis**: Temporal trends in ratings (monthly, yearly, rolling averages).
  * **Bias Detection**: Correlation between review length and rating.
* **Visualization-Ready**: Helper modules generate PNG charts for embedding in dashboards or reports.

---

## 📂 Repository Structure

```bash
├── conf/                      # Hadoop & Spark configuration files
├── data/                      # Raw, processed, and analysis results
├── scripts/                   # Automation scripts for setup & execution
├── src/                       # Source code (preprocessing, MapReduce, Spark)
│   ├── preprocessing/         # Data acquisition & cleaning
│   ├── mapreduce/             # Hadoop streaming mappers & reducers
│   └── spark/                 # Spark jobs, utils, and visualization helper
├── requirements_spark.txt     # Python dependencies
├── run_mapreduce_jobs.sh      # Launch MapReduce pipeline
├── optimize_mapreduce.sh      # Tuning Hadoop performance
└── parquet_to_json_converter_standalone.py  # Parquet → JSON converter
```

---

## 🔧 Installation & Setup

1. **Clone the repository**:

   ```bash
   git clone https://github.com/your-org/amazon-reviews-analysis.git
   cd amazon-reviews-analysis
   ```

2. **Create a Python virtual environment**:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements_spark.txt
   ```

3. **Configure Hadoop & Spark** (modify `conf/` files for your cluster):

   ```bash
   scripts/configure_hadoop.sh
   scripts/start_services.sh
   ```

4. **Convert Parquet to JSON**:

   ```bash
   python parquet_to_json_converter_standalone.py Electronics
   ```

5. **Upload raw data to HDFS**:

   ```bash
   python src/hdfs/hdfs_upload.py
   ```

---

## ⚙️ Usage

### 1. Preprocessing

```bash
scripts/run_preprocessing.sh
scripts/run_validation.sh
```

### 2. MapReduce Analysis

```bash
scripts/run_mapreduce_jobs.sh
```

### 3. Spark Analytics & Visualization

```bash
scripts/run_spark_analysis.sh
```

Results will be stored under `data/spark_results/` and `data/mapreduce_results/` for review and integration.

---

## 🎯 Sample Insights

* **Average Rating by Month**: `data/spark_results/all_categories/trends/monthly_rating_trend.png`
* **Top 10 Products by Popularity**: `data/mapreduce_results/all_reviews_top_analysis_results.txt`
* **Review Length vs. Rating Correlation**: `data/spark_results/all_categories/bias/correlation.txt`

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m "Add new feature"`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

Please follow the existing code style and write tests for new functionality.

---

## 📄 License

This project is licensed under the **MIT License**. See `LICENSE` for details.
