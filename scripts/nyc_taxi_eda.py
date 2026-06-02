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
