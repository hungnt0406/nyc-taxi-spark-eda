# NYC Taxi Spark EDA

PySpark exploratory data analysis for NYC Yellow Taxi trip data, designed to run on Google Cloud Dataproc Serverless with input and output stored in Google Cloud Storage.

## What This Project Does

The Spark job in `scripts/nyc_taxi_eda.py` reads NYC Yellow Taxi Parquet files and writes EDA outputs for:

- Basic row, date, distance, fare, and total amount summaries
- Approximate distinct counts
- Approximate percentiles
- Missing-value counts
- Data quality checks
- IQR-based outlier summaries
- Top total amount outliers
- Hourly and daily summaries
- Stratified payment-type sample
- Correlation matrix

Generated findings reports are stored in `reports/`.

## Project Structure

```text
.
├── scripts/
│   └── nyc_taxi_eda.py
├── reports/
│   ├── eda_findings.md
│   └── eda_findings_2025.md
├── gcp_dataproc_spark_eda_tutorial.md
├── README.md
└── .gitignore
```

Local `data/` folders are ignored by Git because they contain downloaded datasets.

## Requirements

- Google Cloud project with billing enabled
- Google Cloud CLI authenticated locally
- Enabled APIs:
  - `dataproc.googleapis.com`
  - `storage.googleapis.com`
  - `compute.googleapis.com`
- A GCS bucket for raw data, job files, and outputs

Set these variables before running commands:

```bash
export PROJECT_ID=nyc-taxi-spark-eda
export REGION=us-central1
export BUCKET=nyc-taxi-spark-eda

gcloud config set project $PROJECT_ID
```

## Upload the Spark Job

Dataproc Serverless runs the script from Cloud Storage, so upload the local script whenever it changes:

```bash
gcloud storage cp scripts/nyc_taxi_eda.py \
  gs://$BUCKET/jobs/nyc_taxi_eda.py
```

## Run on One Month

If the January 2023 sample file is already in GCS:

```bash
gcloud dataproc batches submit pyspark \
  gs://$BUCKET/jobs/nyc_taxi_eda.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --batch=nyc-taxi-eda-jan-2023 \
  -- \
  --input=gs://$BUCKET/raw/yellow_taxi/yellow_tripdata_2023-01.parquet \
  --output=gs://$BUCKET/outputs/eda_run_2023_01
```

If rerunning, use a new `--batch` name because Dataproc batch IDs cannot be reused.

## Run on Full-Year 2025 Data

The 2025 data is expected at:

```text
gs://nyc-taxi-spark-eda/raw/yellow_taxi/year=2025/
```

Run the full-year EDA:

```bash
gcloud dataproc batches submit pyspark \
  gs://$BUCKET/jobs/nyc_taxi_eda.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --batch=nyc-taxi-eda-2025 \
  -- \
  --input="gs://$BUCKET/raw/yellow_taxi/year=2025/*.parquet" \
  --output=gs://$BUCKET/outputs/eda_run_2025
```

Check batch status:

```bash
gcloud dataproc batches describe nyc-taxi-eda-2025 \
  --project=$PROJECT_ID \
  --region=$REGION \
  --format="yaml(state,stateMessage,createTime)"
```

Expected successful state:

```text
state: SUCCEEDED
```

## Download Results Locally

Download the generated Spark outputs:

```bash
mkdir -p outputs/eda_run_2025

gcloud storage rsync --recursive \
  gs://$BUCKET/outputs/eda_run_2025 \
  outputs/eda_run_2025
```

View a summary CSV:

```bash
cat outputs/eda_run_2025/basic_summary/*.csv
```

Spark also writes `_SUCCESS` marker files. Those are normal and are ignored by Git.

## Main Output Folders

```text
outputs/eda_run_2025/basic_summary/
outputs/eda_run_2025/quality_report/
outputs/eda_run_2025/approx_percentiles/
outputs/eda_run_2025/outlier_summary/
outputs/eda_run_2025/correlation_matrix/
outputs/eda_run_2025/hourly_summary/
outputs/eda_run_2025/daily_summary_csv/
outputs/eda_run_2025/top_outliers/
```

## Findings Reports

Existing reports:

- `reports/eda_findings.md`: January 2023 sample findings
- `reports/eda_findings_2025.md`: Full-year 2025 findings

To refresh a report, rerun the Dataproc job, download the output folder, then read the CSVs under `outputs/eda_run_2025/` and update the corresponding Markdown report.

## Notes

- Do not commit raw Parquet data or generated Spark outputs.
- Keep `scripts/nyc_taxi_eda.py` in sync with the copy uploaded to `gs://$BUCKET/jobs/`.
- Use unique Dataproc batch IDs for each run.
- For full-year data, expect longer runtime than the one-month tutorial run.
