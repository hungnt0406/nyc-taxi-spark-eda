# NYC Taxi Spark EDA Findings - 2025

## Dataset

- Dataset: NYC Yellow Taxi Trip Data
- Files analyzed: 12 monthly Parquet files for 2025
- Processing engine: Dataproc Serverless Spark
- Storage: Google Cloud Storage
- Local result folder: `outputs/eda_run_2025`

## Summary

- Total rows processed: 48,722,602
- Min pickup datetime: 2007-12-05T18:45:00.000
- Max pickup datetime: 2025-12-31T23:59:59.000
- Average trip distance: 6.84 miles
- Average total amount: 26.91
- Median total amount: 21.35
- 95th percentile total amount: 77.11
- 99th percentile total amount: 104.30

The run processed the full 2025 dataset, but a small number of records have pickup dates before 2025. The oldest pickup timestamp is from 2007, so the raw data should be filtered by pickup date before producing a cleaned yearly table.

## Data Quality Findings

### Finding 1: Negative fare and total amount records

Observation:

There are 2,848,620 records with negative `fare_amount`, which is 5.85% of all rows. There are also 973,721 records with negative `total_amount`, which is 2.00% of all rows. The minimum fare is -1807.60 and the minimum total amount is -1832.85.

Impact:

Negative amounts can represent voids, corrections, refunds, or bad records. They materially affect yearly revenue totals, average fare metrics, and model training.

Recommended action:

Keep these records in the raw table, but add quality flags. For ordinary completed-trip analytics, filter to `fare_amount >= 0` and `total_amount >= 0`.

### Finding 2: Zero-distance trips

Observation:

There are 1,402,958 zero-distance trips, which is 2.88% of the dataset. Some high-value outliers also have zero or very short distances, including a 5-second trip with `trip_distance = 0.0` and `total_amount = 3240.61`.

Impact:

Zero-distance trips can include canceled rides, adjustments, bad meter entries, or non-trip transactions. They distort distance-based metrics and can break speed calculations.

Recommended action:

Add a `zero_distance_flag`. Exclude zero-distance trips from distance, speed, and fare-per-mile analysis unless they are being studied separately.

### Finding 3: Invalid timestamps

Observation:

There are 2,235 records where dropoff time is earlier than pickup time. The daily summary also shows pickup dates outside 2025: 2007-12-05, 2008-12-31, 2009-01-01, and 2024-12-31.

Impact:

Invalid timestamps make trip duration and time-pattern analysis unreliable. Out-of-year records also contaminate yearly reporting.

Recommended action:

Filter cleaned yearly analytics to pickup timestamps from 2025-01-01 through 2025-12-31. Flag or remove records where `tpep_dropoff_datetime < tpep_pickup_datetime`.

### Finding 4: Missing passenger, rate, and surcharge fields

Observation:

There are 11,611,894 missing values in each of `passenger_count`, `RatecodeID`, `congestion_surcharge`, and `airport_fee`. That is 23.83% of all records. There are also 146 records with passenger counts above 6.

Impact:

Missing fields affect grouping, fare component analysis, and data quality checks. Suspicious passenger counts can distort passenger-level metrics.

Recommended action:

Use null-safe aggregations, preserve missing values as an explicit category, and add a `passenger_count_valid` flag for expected passenger counts.

## Distribution Findings

### Total amount distribution

- 25th percentile: 15.73
- Median: 21.35
- 75th percentile: 30.75
- 95th percentile: 77.11
- 99th percentile: 104.30
- Average: 26.91
- Minimum: -1832.85
- Maximum: 863380.37

Interpretation:

Most trips are ordinary low-to-medium fare trips. Half of all trips cost 21.35 or less, and 75% cost 30.75 or less. The average is higher than the median, showing right skew, but the average is not trustworthy by itself because the dataset contains both negative amounts and extreme high-value outliers.

## Outlier Findings

- IQR lower bound: -6.85
- IQR upper bound: 53.15
- Outlier count: 5,405,103
- Outlier percentage: 11.09%

Interpretation:

Using the IQR rule, records above 53.15 in `total_amount` are outliers. Some may be valid long-distance or airport-related trips, but the largest records are clearly suspicious and should not be mixed into normal trip analysis without review.

Largest observed total amount records:

| Pickup datetime | Dropoff datetime | Distance | Fare | Total | Payment type | PU | DO | Interpretation |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 2025-01-20T12:07:18.000 | 2025-01-20T12:12:42.000 | 1.60 | 863372.12 | 863380.37 | 4 | 138 | 8 | Invalid or severe fare-entry error |
| 2025-06-11T14:41:03.000 | 2025-06-11T16:14:37.000 | 20.60 | 325478.05 | 325528.45 | 2 | 161 | 132 | Invalid or severe fare-entry error |
| 2025-09-02T16:01:28.000 | 2025-09-02T17:31:24.000 | 11.80 | 323800.27 | 323820.17 | 2 | 132 | 164 | Invalid or severe fare-entry error |
| 2025-02-21T17:28:43.000 | 2025-02-21T17:53:04.000 | 2.20 | 132531.36 | 132555.41 | 3 | 161 | 246 | Invalid or severe fare-entry error |
| 2025-03-09T00:34:23.000 | 2025-03-09T00:40:49.000 | 1191.90 | 46263.88 | 46269.44 | 2 | 229 | 262 | Suspicious distance and fare combination |
| 2025-07-17T14:30:01.000 | 2025-07-17T15:01:21.000 | 8.63 | 37.30 | 5297.87 | 2 | 138 | 264 | Suspicious total amount compared with fare |

## Correlation Findings

- `trip_distance` vs `fare_amount`: 0.007
- `fare_amount` vs `tip_amount`: 0.071
- `passenger_count` vs `total_amount`: 0.008
- `fare_amount` vs `total_amount`: 0.999

Interpretation:

The correlation between `fare_amount` and `total_amount` is almost perfect because total amount includes fare amount. Other correlations are very weak in the raw dataset. The weak distance-to-fare relationship is a warning sign that extreme distances, flat fares, missing fields, and bad records are overwhelming the raw correlation calculation. These correlations should be recalculated after filtering invalid and extreme records.

## Time Pattern Findings

- Pickup hour with the most trips: 18:00, with 3,473,210 trips
- Pickup hour with the highest average total amount: 05:00, with an average total amount of 31.26
- Pickup hour with the lowest average total amount: 02:00, with an average total amount of 23.20
- Date with the most trips: 2025-11-01, with 183,035 trips
- Date with the highest total revenue: 2025-12-11, with 6,053,412.79 total amount

Interpretation:

Trip volume peaks around 18:00, which matches evening commute behavior. The highest average total amount occurs around 05:00, likely because that hour has fewer trips and a higher share of longer airport or early-morning trips.

## Step 18 Answers

### Dataset overview

- Rows processed: 48,722,602
- Min pickup date: 2007-12-05T18:45:00.000
- Max pickup date: 2025-12-31T23:59:59.000

### Data quality

- Negative fares: yes, 2,848,620 records
- Negative distances: no, 0 records
- Zero-distance trips: yes, 1,402,958 records
- Dropoff before pickup: yes, 2,235 records
- Suspicious passenger counts: yes, 146 records above 6

### Distribution

- Median total amount: 21.35
- 95th percentile total amount: 77.11
- 99th percentile total amount: 104.30
- Average total amount compared with median: the average is 26.91, which is higher than the median. The distribution is right-skewed, but the raw average is affected by bad negative and extreme high-value records.

### Outliers

- Percent of trips that are total amount outliers: 11.09%
- Largest total amount record: 863380.37 for a 1.6-mile trip
- Valid or suspicious: many of the largest records are suspicious and should be excluded from normal fare analysis.

### Correlation

- `trip_distance` has almost no raw correlation with `fare_amount`: 0.007
- `fare_amount` has weak raw correlation with `tip_amount`: 0.071
- `passenger_count` has almost no correlation with `total_amount`: 0.008

### Time pattern

- Pickup hour with the most trips: 18:00
- Pickup hour with the highest average fare: 05:00

## Recommended Cleaned Table Rules

1. Keep only pickup timestamps from 2025-01-01 through 2025-12-31 for the 2025 analytics table.
2. Flag records with negative `fare_amount` or negative `total_amount`.
3. Flag records with `trip_distance = 0`.
4. Remove or flag records where dropoff time is earlier than pickup time.
5. Add an outlier flag for `total_amount > 53.15`.
6. Recompute summary statistics and correlations after cleaning.

## Next Steps

1. Build a cleaned Silver table for 2025.
2. Re-run the EDA on cleaned data.
3. Compare 2025 with the January 2023 sample.
4. Add visualizations for hourly and daily trends.
5. Add automated data quality checks to the Spark job.
