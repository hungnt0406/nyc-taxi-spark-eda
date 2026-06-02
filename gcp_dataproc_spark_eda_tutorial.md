# GCP + Spark Project Tutorial: Large-Scale EDA on NYC Taxi Data

This tutorial teaches you how to set up a real cloud-based Spark project on Google Cloud Platform using:

- **Google Cloud Storage (GCS)** as the data lake storage layer
- **Dataproc Serverless** as the managed Spark execution layer
- **PySpark** as the processing engine
- **NYC Taxi Trip Parquet data** as the real large dataset

The project is designed for **Section 5.1: Exploratory Data Analysis at Scale**.

By the end, you will run a real PySpark EDA job on GCP and produce scalable EDA outputs such as summary statistics, missing-data reports, outlier reports, approximate percentiles, correlation matrices, and sampled datasets.

---

## 0. Big Picture: What You Are Building

You are building this architecture:

```text
NYC Taxi Parquet files
        ↓
Google Cloud Storage bucket
        ↓
Dataproc Serverless Spark job
        ↓
EDA output tables in GCS
        ↓
Local inspection / visualization / report
```

In simple words:

1. You store raw data in **GCS**.
2. You submit a **PySpark job** to Dataproc Serverless.
3. Spark reads the Parquet files from GCS.
4. Spark performs large-scale EDA.
5. Spark writes small summary outputs back to GCS.
6. You download or inspect the result files.

This is close to how real production data engineering projects work.

---

## 1. Why We Use Spark Instead of BigQuery Here

BigQuery is great for SQL analytics, but your goal is to practice **Spark-based large-scale EDA**.

Spark gives you practice with:

- Distributed DataFrames
- Partitioned file processing
- Parquet reads and writes
- PySpark transformations and actions
- Cluster/serverless job submission
- Cloud data lake style workflows

This project directly maps to Section 5.1 concepts:

| 5.1 Concept | Project Implementation |
|---|---|
| Distributed computing | Dataproc Serverless Spark |
| Large dataset | NYC Taxi Parquet files |
| Summary statistics | `count`, `avg`, `stddev`, `min`, `max` |
| Approximation algorithms | `approx_count_distinct`, `percentile_approx` |
| Missing data analysis | Null count per column |
| Outlier detection | IQR method using approximate quantiles |
| Stratified sampling | `sampleBy("payment_type")` |
| Correlation analysis | Spark ML `VectorAssembler` + `Correlation` |
| Incremental-style summaries | Daily/hourly summary outputs |

---

## 2. Important Cloud Concepts Before You Start

### 2.1 GCP Project

A **GCP project** is the top-level container for your cloud resources.

It contains things like:

- GCS buckets
- Dataproc jobs
- APIs
- IAM permissions
- Billing records

Every GCP resource belongs to a project.

Example project ID:

```text
nyc-taxi-spark-eda-428901
```

The **project name** can be human-friendly, but the **project ID** is what you use in commands.

---

### 2.2 Region

A **region** is the physical cloud location where resources run.

Example:

```text
us-central1
```

For this learning project, use:

```bash
export REGION="us-central1"
```

Why region matters:

- Your Spark job runs in a region.
- Your GCS bucket should be near that region.
- Keeping storage and compute near each other can reduce latency and avoid location issues.

---

### 2.3 GCS Bucket

A **GCS bucket** is cloud object storage.

Think of it like a cloud folder, but technically it stores objects, not normal local files.

Your bucket will contain:

```text
gs://your-bucket/
├── raw/
│   └── yellow_taxi/
├── jobs/
└── outputs/
```

Meaning:

- `raw/` stores source data.
- `jobs/` stores your PySpark script.
- `outputs/` stores Spark results.

---

### 2.4 Dataproc Serverless

**Dataproc Serverless** runs Spark jobs without requiring you to manually create a Spark cluster.

Traditional Dataproc cluster flow:

```text
Create cluster → Run job → Delete cluster
```

Dataproc Serverless flow:

```text
Submit Spark batch job → GCP manages compute → Job finishes
```

Why this is good for learning:

- Less infrastructure setup
- Lower risk of forgetting to delete a cluster
- Easier to focus on Spark code

---

### 2.5 PySpark Job

A **PySpark job** is a Python script that uses Spark.

Example:

```python
spark.read.parquet("gs://bucket/raw/yellow_taxi/*.parquet")
```

This reads Parquet files from GCS into a distributed Spark DataFrame.

---

## 3. Cost Safety Before Doing Anything

Cloud resources can cost money, so set safety rules.

### Recommended budget

Create a budget alert in GCP:

```text
Billing → Budgets & alerts → Create budget
```

Recommended amount:

```text
$5 or $10
```

Recommended alert thresholds:

```text
50%
90%
100%
```

Important:

> Budget alerts notify you, but they do not automatically stop all spending.

For Dataproc Serverless, you do not manually leave a cluster running, but you can still spend money when jobs run or when data is stored in GCS.

---

## 4. Step 1 — Create a GCP Project in the Web UI

You must create the project first in the Google Cloud Console.

Go to:

```text
https://console.cloud.google.com
```

Then:

```text
Top project dropdown → New Project
```

Use:

```text
Project name: nyc-taxi-spark-eda
```

GCP will generate a unique project ID.

Example:

```text
Project name: nyc-taxi-spark-eda
Project ID: nyc-taxi-spark-eda-428901
```

Save the **Project ID**. You need it in the terminal.

---

## 5. Step 2 — Open Cloud Shell or Local Terminal

You have two options.

### Option A: Use Google Cloud Shell

Cloud Shell is easiest because `gcloud` is already installed.

In Google Cloud Console:

```text
Top-right terminal icon → Activate Cloud Shell
```

Use this if you do not want to install anything locally yet.

### Option B: Use your local terminal

Use this if you already installed Google Cloud CLI locally.

Check with:

```bash
gcloud --version
```

If it prints version info, you are ready.

If not, use Cloud Shell first.

---

## 6. Step 3 — Set Environment Variables

In Cloud Shell or your terminal, set these variables.

Replace the project ID with your real one:

```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export BUCKET="taxi-eda-$PROJECT_ID"
```

Example:

```bash
export PROJECT_ID="nyc-taxi-spark-eda-428901"
export REGION="us-central1"
export BUCKET="taxi-eda-$PROJECT_ID"
```

### Why use variables?

Instead of typing your project ID and bucket name again and again, you store them once.

Then this:

```bash
gcloud config set project $PROJECT_ID
```

means:

```bash
gcloud config set project nyc-taxi-spark-eda-428901
```

### Check your variables

Run:

```bash
echo $PROJECT_ID
echo $REGION
echo $BUCKET
```

Expected output:

```text
nyc-taxi-spark-eda-428901
us-central1
taxi-eda-nyc-taxi-spark-eda-428901
```

---

## 7. Step 4 — Set the Active GCP Project

Run:

```bash
gcloud config set project $PROJECT_ID
```

### What this does

This tells the `gcloud` CLI:

> Use this project as the default project for future commands.

Without this, your commands may run against the wrong project.

### Verify

Run:

```bash
gcloud config list
```

Look for:

```text
project = your-gcp-project-id
```

---

## 8. Step 5 — Enable Required APIs

Run:

```bash
gcloud services enable dataproc.googleapis.com

gcloud services enable storage.googleapis.com

gcloud services enable compute.googleapis.com
```

### What are APIs?

In GCP, services are disabled by default for safety.

You enable APIs to allow your project to use specific services.

### What each API is for

| API | Why you need it |
|---|---|
| `dataproc.googleapis.com` | Run Dataproc Serverless Spark jobs |
| `storage.googleapis.com` | Create and use GCS buckets |
| `compute.googleapis.com` | Dataproc uses compute infrastructure behind the scenes |

If these APIs are not enabled, job submission or bucket creation may fail.

---

## 9. Step 6 — Create a GCS Bucket

Run:

```bash
gcloud storage buckets create gs://$BUCKET --location=$REGION
```

### What this does

It creates a cloud storage bucket like:

```text
gs://taxi-eda-nyc-taxi-spark-eda-428901
```

### Why the bucket name includes project ID

GCS bucket names must be globally unique across all Google Cloud users.

A simple name like:

```text
taxi-eda
```

is probably already taken.

This is more likely to be unique:

```text
taxi-eda-nyc-taxi-spark-eda-428901
```

---

## 10. Step 7 — Create Folder Structure in GCS

Run:

```bash
gcloud storage folders create gs://$BUCKET/raw/
gcloud storage folders create gs://$BUCKET/jobs/
gcloud storage folders create gs://$BUCKET/outputs/
```

### What these folders mean

```text
raw/      raw source data
jobs/     PySpark script files
outputs/  Spark result files
```

### Note about GCS folders

GCS does not have real folders like your laptop.

It uses object prefixes.

But the console and CLI show them like folders, which is convenient.

---

## 11. Step 8 — Create a Local Project Folder

Run:

```bash
mkdir -p nyc-taxi-spark-eda/scripts
mkdir -p nyc-taxi-spark-eda/data
mkdir -p nyc-taxi-spark-eda/reports
cd nyc-taxi-spark-eda
```

### What this creates

```text
nyc-taxi-spark-eda/
├── scripts/
├── data/
└── reports/
```

### Why local folders matter

Even though the job runs on GCP, you still need a clean local project structure for:

- scripts
- SQL or PySpark code
- notes
- reports
- future GitHub repo

---

## 12. Step 9 — Download Real NYC Taxi Data

Download January 2023 Yellow Taxi data:

```bash
curl -L -o data/yellow_tripdata_2023-01.parquet \
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
```

### What this does

It downloads a real NYC Taxi Parquet file into:

```text
data/yellow_tripdata_2023-01.parquet
```

### Why only one month first?

Start small so you can test the pipeline cheaply and quickly.

Once one month works, you can scale to more months.

### Why Parquet?

Parquet is a columnar file format.

Spark can read only the columns it needs, which is more efficient than reading full CSV rows.

Parquet is commonly used in data lakes and production data platforms.

---

## 13. Step 10 — Upload Data to GCS

Run:

```bash
gcloud storage cp data/yellow_tripdata_2023-01.parquet \
  gs://$BUCKET/raw/yellow_taxi/
```

### What this does

It copies the local Parquet file into your cloud bucket.

Destination:

```text
gs://your-bucket/raw/yellow_taxi/yellow_tripdata_2023-01.parquet
```

### Verify upload

Run:

```bash
gcloud storage ls gs://$BUCKET/raw/yellow_taxi/
```

Expected output:

```text
gs://your-bucket/raw/yellow_taxi/yellow_tripdata_2023-01.parquet
```

---

## 14. Step 11 — Create the PySpark EDA Script

Create the script file:

```bash
nano scripts/nyc_taxi_eda.py
```

Paste this code:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import argparse


def write_single_csv(df, path):
    """
    Write a small Spark DataFrame as a single CSV folder.

    Spark normally writes multiple part files because data is distributed.
    For small reports, coalesce(1) makes the result easier to inspect.
    Do not use coalesce(1) for huge outputs.
    """
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("NYC Taxi EDA at Scale")
        .getOrCreate()
    )

    df = spark.read.parquet(args.input)

    base_df = df.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    )

    basic_summary = base_df.select(
        F.count("*").alias("total_rows"),
        F.min("tpep_pickup_datetime").alias("min_pickup_datetime"),
        F.max("tpep_pickup_datetime").alias("max_pickup_datetime"),
        F.mean("trip_distance").alias("avg_trip_distance"),
        F.stddev("trip_distance").alias("std_trip_distance"),
        F.min("trip_distance").alias("min_trip_distance"),
        F.max("trip_distance").alias("max_trip_distance"),
        F.mean("fare_amount").alias("avg_fare_amount"),
        F.stddev("fare_amount").alias("std_fare_amount"),
        F.min("fare_amount").alias("min_fare_amount"),
        F.max("fare_amount").alias("max_fare_amount"),
        F.mean("total_amount").alias("avg_total_amount"),
        F.stddev("total_amount").alias("std_total_amount"),
        F.min("total_amount").alias("min_total_amount"),
        F.max("total_amount").alias("max_total_amount"),
    )
    write_single_csv(basic_summary, f"{args.output}/basic_summary")

    approx_distinct = base_df.select(
        F.approx_count_distinct("VendorID").alias("approx_vendor_count"),
        F.approx_count_distinct("PULocationID").alias("approx_pickup_location_count"),
        F.approx_count_distinct("DOLocationID").alias("approx_dropoff_location_count"),
        F.approx_count_distinct("payment_type").alias("approx_payment_type_count"),
    )
    write_single_csv(approx_distinct, f"{args.output}/approx_distinct")

    approx_percentiles = base_df.select(
        F.expr("percentile_approx(total_amount, 0.25)").alias("total_amount_p25"),
        F.expr("percentile_approx(total_amount, 0.50)").alias("total_amount_p50"),
        F.expr("percentile_approx(total_amount, 0.75)").alias("total_amount_p75"),
        F.expr("percentile_approx(total_amount, 0.95)").alias("total_amount_p95"),
        F.expr("percentile_approx(total_amount, 0.99)").alias("total_amount_p99"),
        F.expr("percentile_approx(trip_distance, 0.25)").alias("trip_distance_p25"),
        F.expr("percentile_approx(trip_distance, 0.50)").alias("trip_distance_p50"),
        F.expr("percentile_approx(trip_distance, 0.75)").alias("trip_distance_p75"),
        F.expr("percentile_approx(trip_distance, 0.95)").alias("trip_distance_p95"),
        F.expr("percentile_approx(trip_distance, 0.99)").alias("trip_distance_p99"),
        F.expr("percentile_approx(fare_amount, 0.25)").alias("fare_amount_p25"),
        F.expr("percentile_approx(fare_amount, 0.50)").alias("fare_amount_p50"),
        F.expr("percentile_approx(fare_amount, 0.75)").alias("fare_amount_p75"),
        F.expr("percentile_approx(fare_amount, 0.95)").alias("fare_amount_p95"),
        F.expr("percentile_approx(fare_amount, 0.99)").alias("fare_amount_p99"),
    )
    write_single_csv(approx_percentiles, f"{args.output}/approx_percentiles")

    missing_exprs = [
        F.count(F.when(F.col(c).isNull(), c)).alias(f"{c}_missing_count")
        for c in base_df.columns
    ]
    missing_counts = base_df.select(*missing_exprs)
    write_single_csv(missing_counts, f"{args.output}/missing_counts")

    quality_report = base_df.select(
        F.count("*").alias("total_rows"),
        F.count(F.when(F.col("trip_distance") < 0, True)).alias("negative_distance"),
        F.count(F.when(F.col("trip_distance") == 0, True)).alias("zero_distance"),
        F.count(F.when(F.col("fare_amount") < 0, True)).alias("negative_fare"),
        F.count(F.when(F.col("total_amount") < 0, True)).alias("negative_total"),
        F.count(F.when(F.col("passenger_count") < 0, True)).alias("negative_passenger_count"),
        F.count(F.when(F.col("passenger_count") > 6, True)).alias("suspicious_passenger_count"),
        F.count(F.when(F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"), True)).alias("dropoff_before_pickup"),
    )
    write_single_csv(quality_report, f"{args.output}/quality_report")

    q1, q3 = base_df.approxQuantile("total_amount", [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outlier_df = base_df.withColumn(
        "is_total_amount_outlier",
        (F.col("total_amount") < lower_bound) | (F.col("total_amount") > upper_bound)
    )

    outlier_summary = outlier_df.select(
        F.lit(q1).alias("q1"),
        F.lit(q3).alias("q3"),
        F.lit(iqr).alias("iqr"),
        F.lit(lower_bound).alias("lower_bound"),
        F.lit(upper_bound).alias("upper_bound"),
        F.count("*").alias("total_rows"),
        F.count(F.when(F.col("is_total_amount_outlier"), True)).alias("outlier_count"),
        (F.count(F.when(F.col("is_total_amount_outlier"), True)) / F.count("*") * 100).alias("outlier_pct"),
    )
    write_single_csv(outlier_summary, f"{args.output}/outlier_summary")

    top_outliers = (
        outlier_df
        .filter(F.col("is_total_amount_outlier"))
        .select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "payment_type",
            "PULocationID",
            "DOLocationID",
        )
        .orderBy(F.col("total_amount").desc())
        .limit(100)
    )
    write_single_csv(top_outliers, f"{args.output}/top_outliers")

    hourly_summary = (
        base_df
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.expr("percentile_approx(total_amount, 0.5)").alias("median_total_amount"),
        )
        .orderBy("pickup_hour")
    )
    write_single_csv(hourly_summary, f"{args.output}/hourly_summary")

    daily_summary = (
        base_df
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("total_amount").alias("total_amount_sum"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("total_amount").alias("avg_total_amount"),
        )
        .orderBy("pickup_date")
    )
    daily_summary.write.mode("overwrite").parquet(f"{args.output}/daily_summary_parquet")
    write_single_csv(daily_summary, f"{args.output}/daily_summary_csv")

    fractions = {
        0: 0.01,
        1: 0.01,
        2: 0.01,
        3: 0.01,
        4: 0.01,
        5: 0.01,
        6: 0.01,
    }

    sampled_df = base_df.sampleBy("payment_type", fractions=fractions, seed=42)
    sampled_df.write.mode("overwrite").parquet(f"{args.output}/stratified_sample_payment_type")

    sample_counts = sampled_df.groupBy("payment_type").count().orderBy("payment_type")
    write_single_csv(sample_counts, f"{args.output}/sample_counts_by_payment_type")

    feature_cols = [
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "passenger_count",
    ]

    clean_features = base_df.select(feature_cols).dropna()

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    vector_df = assembler.transform(clean_features).select("features")

    corr_matrix = Correlation.corr(vector_df, "features", "pearson").collect()[0][0]
    corr_array = corr_matrix.toArray().tolist()

    corr_rows = []
    for i, row_name in enumerate(feature_cols):
        row = {"feature": row_name}
        for j, col_name in enumerate(feature_cols):
            row[col_name] = float(corr_array[i][j])
        corr_rows.append(row)

    corr_df = spark.createDataFrame(corr_rows)
    write_single_csv(corr_df, f"{args.output}/correlation_matrix")

    spark.stop()


if __name__ == "__main__":
    main()
```

Save the file.

If using `nano`:

```text
CTRL + O → Enter → CTRL + X
```

---

## 15. Step 12 — Understand the PySpark Script

### 15.1 Argument parser

```python
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
```

This lets you pass paths from the command line.

Example:

```bash
--input=gs://bucket/raw/yellow_taxi/*.parquet
--output=gs://bucket/outputs/eda_run_001
```

Why this exists:

You should not hardcode paths in production Spark jobs.

Good jobs accept parameters.

---

### 15.2 SparkSession

```python
spark = SparkSession.builder.appName("NYC Taxi EDA at Scale").getOrCreate()
```

This creates the Spark application.

SparkSession is the entry point for:

- Reading data
- Creating DataFrames
- Running SQL-like transformations
- Writing outputs

---

### 15.3 Reading Parquet

```python
df = spark.read.parquet(args.input)
```

This reads Parquet files from GCS.

Because the input path uses `*.parquet`, Spark reads all matching Parquet files.

Example:

```text
gs://bucket/raw/yellow_taxi/*.parquet
```

If there is one file, it reads one file.

If there are twelve monthly files, it reads all twelve.

---

### 15.4 Selecting useful columns

```python
base_df = df.select(...)
```

This avoids carrying unnecessary columns through the job.

Why this matters:

- Less memory use
- Less network shuffle
- Easier code readability
- More stable schema for EDA

---

### 15.5 Summary statistics

```python
F.count("*")
F.mean("trip_distance")
F.stddev("trip_distance")
F.min("trip_distance")
F.max("trip_distance")
```

These tell you the basic shape of numeric columns.

You look for:

- Extreme max values
- Negative values
- Zero values
- Strange averages
- Wide standard deviation

---

### 15.6 Approximate distinct counts

```python
F.approx_count_distinct("PULocationID")
```

This estimates unique values using a memory-efficient algorithm.

Why not always exact `countDistinct`?

Exact distinct counting can be expensive at large scale because Spark may need to shuffle many unique keys.

Approximation is faster and often good enough for EDA.

---

### 15.7 Approximate percentiles

```python
percentile_approx(total_amount, 0.50)
```

This estimates one percentile value at a time. The job writes separate CSV columns for:

- 25th percentile
- median
- 75th percentile
- 95th percentile
- 99th percentile

Why this exists:

Averages can be misleading.

Percentiles show the distribution better.

---

### 15.8 Missing data report

```python
F.count(F.when(F.col(c).isNull(), c))
```

This counts null values per column.

Why this matters:

Missing values can break downstream analytics and ML models.

---

### 15.9 Data quality checks

Examples:

```python
F.col("trip_distance") < 0
F.col("fare_amount") < 0
F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")
```

These are business logic checks.

They ask:

> Does this record make sense?

Examples of suspicious records:

- Negative fare
- Negative distance
- Dropoff before pickup
- Passenger count greater than expected

---

### 15.10 IQR outlier detection

```python
q1, q3 = base_df.approxQuantile("total_amount", [0.25, 0.75], 0.01)
```

This computes approximate Q1 and Q3.

Then:

```python
iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr
```

Records outside this range are marked as outliers.

Why use IQR?

IQR is robust for skewed data, and taxi fares are usually skewed.

---

### 15.11 Hourly and daily summaries

```python
F.hour("tpep_pickup_datetime")
F.to_date("tpep_pickup_datetime")
```

These create time-based aggregations.

They help answer:

- Which hour has the most trips?
- Which day has the highest revenue?
- Does trip volume change over time?

---

### 15.12 Stratified sampling

```python
sampleBy("payment_type", fractions=fractions, seed=42)
```

This samples from each payment type group.

Why this matters:

Random sampling can miss rare categories.

Stratified sampling keeps groups represented.

---

### 15.13 Correlation matrix

```python
VectorAssembler(inputCols=feature_cols, outputCol="features")
Correlation.corr(vector_df, "features", "pearson")
```

Spark ML expects numeric features in one vector column.

Correlation helps identify relationships like:

- Does distance correlate with fare?
- Does fare correlate with tip?
- Does passenger count matter?

---

## 16. Step 13 — Upload the PySpark Job to GCS

Run:

```bash
gcloud storage cp scripts/nyc_taxi_eda.py \
  gs://$BUCKET/jobs/nyc_taxi_eda.py
```

### What this does

Dataproc Serverless needs to access your script from cloud storage.

So you upload it to:

```text
gs://your-bucket/jobs/nyc_taxi_eda.py
```

Verify:

```bash
gcloud storage ls gs://$BUCKET/jobs/
```

Expected:

```text
gs://your-bucket/jobs/nyc_taxi_eda.py
```

---

## 17. Step 14 — Submit the Spark Job

Run:

```bash
gcloud dataproc batches submit pyspark \
  gs://$BUCKET/jobs/nyc_taxi_eda.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --batch=nyc-taxi-eda-001 \
  -- \
  --input=gs://$BUCKET/raw/yellow_taxi/*.parquet \
  --output=gs://$BUCKET/outputs/eda_run_001
```

### What this command means

```bash
gcloud dataproc batches submit pyspark
```

Submit a PySpark batch job.

```bash
gs://$BUCKET/jobs/nyc_taxi_eda.py
```

This is the PySpark file to run.

```bash
--project=$PROJECT_ID
```

Run inside your GCP project.

```bash
--region=$REGION
```

Run in the selected region.

```bash
--batch=nyc-taxi-eda-001
```

Give this batch job a unique name.

```bash
--
```

Everything after this goes to your Python script, not to `gcloud`.

```bash
--input=gs://$BUCKET/raw/yellow_taxi/*.parquet
```

Input data path.

```bash
--output=gs://$BUCKET/outputs/eda_run_001
```

Output folder.

---

## 18. Step 15 — Monitor the Job

List jobs:

```bash
gcloud dataproc batches list --region=$REGION
```

Describe your job:

```bash
gcloud dataproc batches describe nyc-taxi-eda-001 --region=$REGION
```

Look for job state:

```text
SUCCEEDED
FAILED
RUNNING
PENDING
```

If it fails, read the error message in the batch details.

Common causes:

- Wrong bucket path
- Wrong project ID
- API not enabled
- Permission issue
- Column name mismatch
- Batch name already used

---

## 19. Step 16 — Check Output Files

Run:

```bash
gcloud storage ls gs://$BUCKET/outputs/eda_run_001/
```

Expected output folders:

```text
basic_summary/
approx_distinct/
approx_percentiles/
missing_counts/
quality_report/
outlier_summary/
top_outliers/
hourly_summary/
daily_summary_csv/
daily_summary_parquet/
stratified_sample_payment_type/
sample_counts_by_payment_type/
correlation_matrix/
```

Each folder is an output from one EDA task.

---

## 20. Step 17 — Download and Inspect Results

Create local output folder:

```bash
mkdir -p outputs/basic_summary
```

Download basic summary:

```bash
gcloud storage cp \
  "gs://$BUCKET/outputs/eda_run_001/basic_summary/*.csv" \
  outputs/basic_summary/
```

View result:

```bash
cat outputs/basic_summary/*.csv
```

Download other reports:

```bash
mkdir -p outputs/quality_report outputs/outlier_summary outputs/correlation_matrix

gcloud storage cp "gs://$BUCKET/outputs/eda_run_001/quality_report/*.csv" outputs/quality_report/

gcloud storage cp "gs://$BUCKET/outputs/eda_run_001/outlier_summary/*.csv" outputs/outlier_summary/

gcloud storage cp "gs://$BUCKET/outputs/eda_run_001/correlation_matrix/*.csv" outputs/correlation_matrix/
```

View:

```bash
cat outputs/quality_report/*.csv
cat outputs/outlier_summary/*.csv
cat outputs/correlation_matrix/*.csv
```

---

## 21. Step 18 — Interpret Your First Results

After your first successful run, answer these questions.

### Dataset overview

```text
How many rows were processed?
What is the min pickup date?
What is the max pickup date?
```

### Data quality

```text
Are there negative fares?
Are there negative distances?
Are there zero-distance trips?
Are there dropoff times before pickup times?
Are there suspicious passenger counts?
```

### Distribution

```text
What is the median total amount?
What is the 95th percentile total amount?
What is the 99th percentile total amount?
Is the average much larger than the median?
```

### Outliers

```text
What percent of trips are total_amount outliers?
What are the largest total_amount records?
Do they look valid or suspicious?
```

### Correlation

```text
Does trip_distance correlate with fare_amount?
Does fare_amount correlate with tip_amount?
Does passenger_count correlate with total_amount?
```

### Time pattern

```text
Which pickup hour has the most trips?
Which pickup hour has the highest average fare?
```

---

## 22. Step 19 — Create a Findings Report

Create:

```bash
nano reports/eda_findings.md
```

Use this template:

```markdown
# NYC Taxi Spark EDA Findings

## Dataset

- Dataset: NYC Yellow Taxi Trip Data
- Files analyzed: January 2023
- Processing engine: Dataproc Serverless Spark
- Storage: Google Cloud Storage

## Summary

- Total rows processed:
- Min pickup datetime:
- Max pickup datetime:
- Average trip distance:
- Average total amount:

## Data Quality Findings

### Finding 1: Negative fare records

Observation:

Impact:

Recommended action:

### Finding 2: Zero-distance trips

Observation:

Impact:

Recommended action:

### Finding 3: Dropoff before pickup

Observation:

Impact:

Recommended action:

## Distribution Findings

### Total amount distribution

- Median:
- 95th percentile:
- 99th percentile:

Interpretation:

## Outlier Findings

- IQR lower bound:
- IQR upper bound:
- Outlier count:
- Outlier percentage:

Interpretation:

## Correlation Findings

- trip_distance vs fare_amount:
- fare_amount vs tip_amount:
- passenger_count vs total_amount:

Interpretation:

## Next Steps

1. Analyze more months.
2. Add visualization notebook.
3. Build cleaned Silver table.
4. Compare monthly trends.
5. Add automated data quality checks.
```

This turns raw EDA results into communication, which is what real data engineers and analysts do.

---

## 23. Step 20 — Scale to More Data

After one month works, download more months:

```bash
curl -L -o data/yellow_tripdata_2023-02.parquet \
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet

curl -L -o data/yellow_tripdata_2023-03.parquet \
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet
```

Upload:

```bash
gcloud storage cp data/yellow_tripdata_2023-*.parquet \
  gs://$BUCKET/raw/yellow_taxi/
```

Submit a new batch:

```bash
gcloud dataproc batches submit pyspark \
  gs://$BUCKET/jobs/nyc_taxi_eda.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --batch=nyc-taxi-eda-002 \
  -- \
  --input=gs://$BUCKET/raw/yellow_taxi/*.parquet \
  --output=gs://$BUCKET/outputs/eda_run_002
```

Important:

> Batch names must be unique. You cannot reuse `nyc-taxi-eda-001`.

---

## 24. Common Errors and Fixes

### Error: Bucket already exists

Problem:

```text
Bucket name is already taken globally.
```

Fix:

Use a more unique bucket name:

```bash
export BUCKET="taxi-eda-$PROJECT_ID-$RANDOM"
```

Then create the bucket again.

---

### Error: API not enabled

Problem:

Dataproc or Storage API is not enabled.

Fix:

```bash
gcloud services enable dataproc.googleapis.com

gcloud services enable storage.googleapis.com

gcloud services enable compute.googleapis.com
```

---

### Error: Batch already exists

Problem:

You reused the same batch name.

Fix:

Use a new batch name:

```bash
--batch=nyc-taxi-eda-002
```

---

### Error: File not found

Problem:

Input path is wrong or data was not uploaded.

Check:

```bash
gcloud storage ls gs://$BUCKET/raw/yellow_taxi/
```

Fix the input path if needed.

---

### Error: Column does not exist

Problem:

The Parquet schema changed or column names differ.

Check schema locally or with Spark.

Common issue:

Some datasets may have columns like `airport_fee` instead of `Airport_fee` depending on year/vendor format.

Fix:

Update the `base_df.select(...)` list to match the actual schema.

---

## 25. Cleanup to Control Cost

Remove old outputs you no longer need:

```bash
gcloud storage rm --recursive gs://$BUCKET/outputs/eda_run_001
```

Remove raw data if you are done:

```bash
gcloud storage rm --recursive gs://$BUCKET/raw/yellow_taxi
```

Delete the bucket only if you are fully done:

```bash
gcloud storage rm --recursive gs://$BUCKET
```

Be careful:

> Deleting the bucket removes all files inside it.

---

## 26. Final Checklist

You are done with setup when you can confirm:

```text
GCP project created
Billing/budget alert configured
Required APIs enabled
GCS bucket created
NYC Taxi Parquet uploaded to GCS
PySpark script uploaded to GCS
Dataproc Serverless batch submitted
Batch succeeded
Outputs created in GCS
At least one CSV report downloaded and inspected
```

---

## 27. What You Learned

By completing this tutorial, you practiced:

- Creating a cloud Spark project
- Using GCS as a data lake
- Running Dataproc Serverless Spark jobs
- Reading real Parquet data with PySpark
- Computing distributed summary statistics
- Using approximation algorithms
- Detecting missing values
- Detecting outliers using IQR
- Building data quality reports
- Producing hourly/daily aggregate summaries
- Creating stratified samples
- Computing correlation matrices at scale
- Writing Spark outputs back to cloud storage

This is a strong foundation for Section 5.1: **Exploratory Data Analysis at Scale**.
