# NYC Taxi Spark EDA Findings

## Dataset

- Dataset: NYC Yellow Taxi Trip Data
- Files analyzed: January 2023
- Processing engine: Dataproc Serverless Spark
- Storage: Google Cloud Storage
- Local result folder: `outputs/eda_run_002`

## Summary

- Total rows processed: 3,066,766
- Min pickup datetime: 2008-12-31T23:01:42.000
- Max pickup datetime: 2023-02-01T00:56:53.000
- Average trip distance: 3.85 miles
- Average total amount: 27.02
- Median total amount: 20.16
- 95th percentile total amount: 80.25
- 99th percentile total amount: 101.94

The dataset is mostly January 2023 yellow taxi data, but the minimum pickup datetime is from 2008 and the maximum pickup datetime reaches early February 2023. Those records should be treated as boundary or data quality cases before building a cleaned table.

## Data Quality Findings

### Finding 1: Negative fare records

Observation:

There are 25,049 records with negative `fare_amount` and 25,204 records with negative `total_amount`. The minimum fare is -900.00 and the minimum total amount is -751.00.

Impact:

Negative amounts can represent voided trips, corrections, refunds, or bad records. They will distort averages, revenue totals, and model training if mixed with normal completed trips.

Recommended action:

Keep these records in the raw table, but flag them in a cleaned table. For normal trip analytics, filter to `fare_amount >= 0` and `total_amount >= 0`.

### Finding 2: Zero-distance trips

Observation:

There are 45,862 records with `trip_distance = 0`. Some of the largest outliers are zero-distance trips with very high fares, including total amounts of 1000.00, 901.00, 751.00, 656.85, 651.00, and 626.00.

Impact:

Zero-distance trips may include canceled rides, corrections, flat charges, bad meter entries, or non-trip transactions. High-value zero-distance rows are especially suspicious and can heavily skew fare analysis.

Recommended action:

Add a data quality flag for `trip_distance = 0`. For distance-based analysis, exclude zero-distance trips or review them separately.

### Finding 3: Dropoff before pickup

Observation:

There are 3 records where `tpep_dropoff_datetime` is earlier than `tpep_pickup_datetime`.

Impact:

These records have invalid trip duration and should not be used for duration, speed, or time-based feature engineering.

Recommended action:

Flag or remove records where `tpep_dropoff_datetime < tpep_pickup_datetime` in the cleaned table.

### Finding 4: Missing and suspicious values

Observation:

There are 71,743 missing values in each of `passenger_count`, `RatecodeID`, `congestion_surcharge`, and `airport_fee`. There are also 20 records with passenger counts above 6.

Impact:

Missing categorical and surcharge fields can affect grouping and fare component analysis. Suspicious passenger counts can distort passenger-based metrics.

Recommended action:

Use null-safe aggregations, keep missing categories explicit, and add a `passenger_count_valid` flag for counts between 0 and 6.

## Distribution Findings

### Total amount distribution

- 25th percentile: 15.40
- Median: 20.16
- 75th percentile: 28.70
- 95th percentile: 80.25
- 99th percentile: 101.94
- Average: 27.02

Interpretation:

The average total amount is higher than the median, which indicates a right-skewed distribution. Most trips are relatively low-cost, but airport trips, long-distance trips, surcharges, and suspicious high-value records pull the average upward.

## Outlier Findings

- IQR lower bound: -4.20
- IQR upper bound: 47.80
- Outlier count: 378,304
- Outlier percentage: 12.34%

Interpretation:

Using the IQR rule, any `total_amount` above 47.80 is treated as an outlier. Some top outliers look potentially valid, such as long trips from pickup zone 132 or to dropoff zone 265. Others look suspicious, especially zero-distance trips with very high total amounts and very short durations.

Largest observed total amount records:

| Pickup datetime | Dropoff datetime | Distance | Fare | Total | Payment type | PU | DO | Interpretation |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 2023-01-24T12:43:44.000 | 2023-01-24T15:41:02.000 | 177.88 | 1160.10 | 1169.40 | 2 | 132 | 265 | Possible long negotiated or out-of-area trip |
| 2023-01-09T16:17:32.000 | 2023-01-09T16:20:41.000 | 0.00 | 999.00 | 1000.00 | 3 | 141 | 141 | Suspicious zero-distance high fare |
| 2023-01-30T13:17:33.000 | 2023-01-30T13:17:48.000 | 0.00 | 900.00 | 901.00 | 3 | 182 | 182 | Suspicious zero-distance high fare |
| 2023-01-30T13:23:56.000 | 2023-01-30T13:24:08.000 | 0.00 | 750.00 | 751.00 | 3 | 182 | 182 | Suspicious zero-distance high fare |
| 2023-01-30T16:17:35.000 | 2023-01-31T08:33:24.000 | 8.81 | 701.60 | 705.60 | 2 | 131 | 265 | Suspicious duration and fare combination |

## Correlation Findings

- `trip_distance` vs `fare_amount`: 0.094
- `fare_amount` vs `tip_amount`: 0.588
- `passenger_count` vs `total_amount`: 0.031

Interpretation:

`fare_amount` and `tip_amount` have a moderate positive relationship. `passenger_count` has almost no relationship with total amount. The weak relationship between `trip_distance` and `fare_amount` is surprising and is likely affected by fare rules, flat fares, extreme distance values, zero-distance outliers, and other dirty records. This should be recalculated after cleaning.

## Time Pattern Findings

- Pickup hour with the most trips: 18:00, with 215,889 trips
- Pickup hour with the highest average total amount: 05:00, with an average total amount of 36.06
- Pickup hour with the lowest average total amount: 02:00, with an average total amount of 24.66

Interpretation:

Trip volume peaks during the evening commute around 18:00. The highest average total amount occurs around 05:00, likely because this hour has fewer trips but a higher share of longer airport or early-morning trips.

## Step 18 Answers

### Dataset overview

- Rows processed: 3,066,766
- Min pickup date: 2008-12-31T23:01:42.000
- Max pickup date: 2023-02-01T00:56:53.000

### Data quality

- Negative fares: yes, 25,049 records
- Negative distances: no, 0 records
- Zero-distance trips: yes, 45,862 records
- Dropoff before pickup: yes, 3 records
- Suspicious passenger counts: yes, 20 records above 6

### Distribution

- Median total amount: 20.16
- 95th percentile total amount: 80.25
- 99th percentile total amount: 101.94
- Average total amount compared with median: the average is 27.02, which is meaningfully higher than the median because the distribution is right-skewed.

### Outliers

- Percent of trips that are total amount outliers: 12.34%
- Largest total amount record: 1169.40 for a 177.88-mile trip from zone 132 to zone 265
- Valid or suspicious: mixed. Some long-distance records may be valid, but zero-distance high-fare records are suspicious.

### Correlation

- `trip_distance` correlates weakly with `fare_amount`: 0.094
- `fare_amount` correlates moderately with `tip_amount`: 0.588
- `passenger_count` has almost no correlation with `total_amount`: 0.031

### Time pattern

- Pickup hour with the most trips: 18:00
- Pickup hour with the highest average fare: 05:00

## Next Steps

1. Analyze more months.
2. Add visualization notebook.
3. Build a cleaned Silver table with invalid records flagged.
4. Compare monthly trends.
5. Add automated data quality checks.
