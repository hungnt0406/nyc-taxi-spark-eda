# Time-Series Analysis & Forecasting Findings

## Dataset

- Source: NYC Yellow Taxi Trip Parquet data
- Time range analyzed for forecasting: 2025-01-01 to 2025-12-31
- Daily rows in Spark time-series output: 369
- Calendar-year 2025 daily rows used by ARIMA and Prophet: 365
- Spark output folder: `outputs/timeseries_run_2025_003`
- ARIMA output folder: `outputs/forecasting_run_2025_001`
- Prophet output folder: `outputs/prophet_run_2025_001`

The Spark time-series output includes four out-of-year dates from the raw data:
2007-12-05, 2008-12-31, 2009-01-01, and 2024-12-31. The ARIMA and Prophet
forecasting batches filtered the series to calendar year 2025 before modeling.

## Time-Series Targets

- Target 1: Daily trip count
- Target 2: Daily total revenue

The forecasting models in sections 38 and 39 forecast `total_trips`, which is
the number of NYC Yellow Taxi trips per day. In plain English, the models are
trying to predict how many yellow taxi rides will happen on the next day based
on the past daily trip-count pattern.

The current ARIMA and Prophet models do not forecast fare amount, total
revenue, trip distance, tips, or passenger count. Revenue is still included in
the Spark daily time-series output for later modeling, but it is not the target
used in the current forecast comparison.

## Trend Findings

- Highest monthly trip volume: 2025-05, with 4,265,938 trips.
- Lowest 2025 monthly trip volume: 2025-08, with 3,312,642 trips.
- Highest monthly revenue: 2025-12, with 134,461,855.82 total revenue.
- Highest daily trip count: 2025-12-12, with 180,748 trips.
- Lowest 2025 daily trip count: 2025-12-25, with 66,961 trips.

Interpretation:

Taxi demand varies meaningfully across the year. May has the highest trip
volume, while December has the highest total revenue. Christmas Day has the
lowest 2025 daily trip count, which is a clear holiday effect that simple
models do not explicitly account for.

## Seasonality Findings

### Hour-of-day pattern

- Busiest pickup hour: 18:00, with 3,259,802 trips.
- Lowest-volume pickup hour: 04:00, with 331,050 trips.
- Highest average total amount hour: 05:00, with an average total amount of
  34.65 and average trip distance of 26.70 miles.

Interpretation:

Trip volume peaks around the evening commute. Early morning has much lower
volume, but the 05:00 hour has the highest average fare and longest average
distance, likely because of airport and early travel trips.

### Day-of-week pattern

- Highest-volume day of week: Thursday, with 7,042,969 trips.
- Lowest-volume day of week: Monday, with 5,627,725 trips.
- Highest average total amount day: Monday, with an average total amount of
  30.07.

Interpretation:

The series has weekly seasonality. Thursday and Saturday are the highest-volume
days, while Monday is the lowest-volume day. This supports using weekday-aware
features and seasonal baselines.

## Data Quality Notes

- The time-series Spark job removes rows with null pickup/dropoff timestamps.
- It removes rows where dropoff is before pickup.
- It removes rows with negative `trip_distance`, `fare_amount`, or
  `total_amount`.
- The raw EDA found out-of-year timestamps, negative amount records, missing
  passenger/rate fields, and extreme fare outliers.

Recommended action:

Keep using a cleaned time-series table for forecasting. For production
forecasting, explicitly filter pickup dates to the target analysis period and
add holiday indicators.

## Forecasting Method Explanations

### Naive Forecast

Formula:

```text
forecast[t] = actual[t-1]
```

Meaning:

Tomorrow will look like yesterday. This is the simplest useful benchmark.

### Seasonal Naive Forecast

Formula for daily data with weekly seasonality:

```text
forecast[t] = actual[t-7]
```

Meaning:

This Monday will look like last Monday. It tests whether weekly seasonality is
strong enough to beat a previous-day forecast.

### Rolling Average Forecast

Formula:

```text
forecast[t] = average(actual[t-7], ..., actual[t-1])
```

Meaning:

Tomorrow will look like the recent weekly average. This smooths noisy daily
values.

### ARIMA

ARIMA means:

```text
AR = uses past values
I  = uses differencing to handle trend
MA = uses past forecast errors
```

This project used `ARIMA(2, 1, 2)`, meaning two autoregressive terms, one
differencing step, and two moving-average terms.

### Prophet

Prophet models a time series as:

```text
value = trend + seasonality + holiday effects + noise
```

This project used weekly seasonality, no yearly seasonality, no daily
seasonality, and additive seasonality.

## Stationarity Tests

| Test | Statistic | P-value | Interpretation |
|---|---:|---:|---|
| ADF | -4.3651 | 0.0003 | Rejects unit root; supports stationarity |
| KPSS | 0.3442 | 0.1000 | Does not reject stationarity at common thresholds |

Interpretation:

Both tests suggest the 2025 daily trip count series is reasonably stationary
after filtering to calendar year 2025. That supports trying classical models
such as ARIMA, though holiday and event effects still matter.

## Forecast Comparison

| Model | MAE | RMSE | MAPE | Notes |
|---|---:|---:|---:|---|
| Naive | 11,435.94 | 13,756.35 | 9.49% | Uses previous day |
| Seasonal naive | 13,850.75 | 21,413.80 | 12.08% | Uses same weekday last week |
| Rolling 7-day average | 13,040.00 | 16,903.63 | 10.99% | Smooth baseline |
| ARIMA(2,1,2) | 20,084.37 | 25,250.52 | 17.22% | Classical statistical model |
| Prophet | 20,422.17 | 30,262.79 | 19.66% | Trend plus weekly seasonality |

Interpretation:

The naive forecast is the best model so far on all three metrics. Seasonal
naive and rolling average do not beat naive, which means short-term day-to-day
persistence is stronger than the simple weekly pattern in this test split.
ARIMA and Prophet do not beat the baselines, so the advanced models are not
worth using yet without more tuning and better features.

## Baseline Forecast Results

| Model | MAE | RMSE | MAPE |
|---|---:|---:|---:|
| Naive | 11,435.94 | 13,756.35 | 9.49% |
| Seasonal naive | 13,850.75 | 21,413.80 | 12.08% |
| Rolling 7-day average | 13,040.00 | 16,903.63 | 10.99% |

## ARIMA Results

- Order used: ARIMA(2,1,2)
- Train rows: 292
- Test rows: 73
- MAE: 20,084.37
- RMSE: 25,250.52
- MAPE: 17.22%

Interpretation:

ARIMA did not beat the baseline forecasts. The starter order is useful as a
first statistical model, but it needs tuning before it is useful for this taxi
demand series.

## Prophet Results

- Seasonality settings: weekly seasonality enabled, yearly seasonality disabled,
  daily seasonality disabled, additive mode
- Train rows: 292
- Test rows: 73
- MAE: 20,422.17
- RMSE: 30,262.79
- MAPE: 19.66%

Interpretation:

Prophet also did not beat the baselines. It may improve with holiday effects,
yearly seasonality once multiple years are available, and regressors such as
weather, airport travel periods, or special events.

## Final Recommendation

- Best model so far: Naive forecast
- Why: It has the lowest MAE, RMSE, and MAPE in the current comparison.
- Next improvement: Add holiday indicators, tune ARIMA orders, test SARIMA,
  and compare models on multiple rolling backtest windows instead of one
  train/test split.

## Updated Project Folder Structure

```text
nyc-taxi-spark-eda/
├── scripts/
│   ├── nyc_taxi_eda.py
│   ├── nyc_taxi_timeseries.py
│   ├── nyc_taxi_forecasting.py
│   └── nyc_taxi_prophet_forecasting.py
├── outputs/
│   ├── eda_run_2025/
│   ├── timeseries_run_2025_003/
│   ├── forecasting_run_2025_001/
│   └── prophet_run_2025_001/
├── reports/
│   ├── eda_findings.md
│   ├── eda_findings_2025.md
│   └── time_series_findings.md
└── README.md
```

## What Was Learned

- How to turn raw taxi events into a daily time series.
- How to create daily, hourly, day-of-week, and monthly aggregates.
- How to create lag and rolling features with Spark window functions.
- How to build naive, seasonal naive, and rolling-average baselines.
- How to run ARIMA and Prophet forecasting in GCP instead of installing
  packages locally.
- How to compare forecasts with MAE, RMSE, and MAPE.
- How to write time-series findings in a report-oriented format.
