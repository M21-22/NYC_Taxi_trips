from pyspark.sql import functions as F

def normalize_trips(df, taxi_type, pickup_column, dropoff_column):
    return (
        df.select(
            "year", "month", "day",
            F.lit(taxi_type).alias("taxi_type"),
            F.col(pickup_column).alias("pickup_datetime"),
            F.col(dropoff_column).alias("dropoff_datetime"),
            "PULocationID", "DOLocationID", "passenger_count", "trip_distance",
            "payment_type", "fare_amount", "tip_amount", "tolls_amount", "total_amount"
        )
        .withColumn("trip_duration_minutes", F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime) / 60.0"))
    )

def valid_trip_condition(pickup_column: str, dropoff_column: str) -> str:
    return f"""
        {pickup_column} IS NOT NULL
        AND {dropoff_column} IS NOT NULL
        AND {dropoff_column} > {pickup_column}
        AND PULocationID IS NOT NULL
        AND DOLocationID IS NOT NULL
        AND PULocationID > 0
        AND DOLocationID > 0
        AND trip_distance >= 0
    """

def aggregate_daily_route_metrics(trips_df, taxi_zones_df):
    pickup_zones_df = taxi_zones_df.select(
        F.col("LocationID").alias("pickup_location_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone")
    )

    dropoff_zones_df = taxi_zones_df.select(
        F.col("LocationID").alias("dropoff_location_id"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone")
    )

    enriched_trips_df = (
        trips_df
        .join(
            F.broadcast(pickup_zones_df),
            F.col("PULocationID") == F.col("pickup_location_id"),
            "left"
        )
        .join(
            F.broadcast(dropoff_zones_df),
            F.col("DOLocationID") == F.col("dropoff_location_id"),
            "left"
        )
        .drop("pickup_location_id", "dropoff_location_id")
    )

    charge = F.col("total_amount").cast("decimal(18,4)")
    tip = F.col("tip_amount").cast("decimal(18,4)")
    fare = F.col("fare_amount").cast("decimal(18,4)")
    distance = F.col("trip_distance")
    duration = F.col("trip_duration_minutes")
    card_paid = F.col("payment_type") == 1
    comparable_card = card_paid & fare.isNotNull() & tip.isNotNull()

    return (
        enriched_trips_df
        .groupBy(
            "year", "month", "day", "taxi_type",
            "PULocationID", "DOLocationID",
            "pickup_borough", "pickup_zone",
            "dropoff_borough", "dropoff_zone",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum(charge).alias("total_passenger_charges"),
            F.count(charge).alias("charge_trip_count"),
            F.sum(tip).alias("total_tips"),
            F.count(tip).alias("tip_trip_count"),
            F.sum(distance).alias("total_trip_distance"),
            F.count(distance).alias("distance_trip_count"),
            F.sum(duration).alias("total_trip_duration_minutes"),
            F.count(duration).alias("duration_trip_count"),
            F.sum(F.when(distance.isNotNull(), charge)).alias("distance_charge_total"),
            F.sum(F.when(charge.isNotNull(), distance)).alias("charge_distance"),
            F.sum(F.when(comparable_card, fare)).alias("credit_card_fare_total"),
            F.sum(F.when(comparable_card, tip)).alias("credit_card_tip_total"),
            F.count(F.when(card_paid, 1)).alias("credit_card_trip_count"),
            F.count(F.when(F.col("payment_type") == 2, 1)).alias("cash_trip_count"),
        )
    )


def _trip_checks(pickup_column: str, dropoff_column: str) -> dict[str, str]:
    return {
        "missing_pickup_timestamp": f"{pickup_column} IS NOT NULL",
        "missing_dropoff_timestamp": f"{dropoff_column} IS NOT NULL",
        "invalid_trip_duration": f"{dropoff_column} > {pickup_column}",
        "missing_pickup_location": "PULocationID IS NOT NULL",
        "missing_dropoff_location": "DOLocationID IS NOT NULL",
        "invalid_pickup_location": "PULocationID > 0",
        "invalid_dropoff_location": "DOLocationID > 0",
        "negative_trip_distance": "trip_distance >= 0",
    }


def prepare_bronze(df, pickup_column: str):
    file_name = F.col("_metadata.file_name")
    source_month = F.regexp_extract(
        file_name, r"^(?:yellow|green)_tripdata_([0-9]{4}-(?:0[1-9]|1[0-2]))[.]parquet$", 1
    )
    return (
        df.drop("customer_first_name", "customer_last_name", "customer_email")
        .withColumns({
            "source_file_path": F.col("_metadata.file_path"),
            "source_file_name": file_name,
            "source_file_size": F.col("_metadata.file_size"),
            "source_file_modified_at": F.col("_metadata.file_modification_time"),
            "source_month": F.when(source_month != "", source_month),
            "year": F.year(pickup_column),
            "month": F.month(pickup_column),
            "day": F.dayofmonth(pickup_column),
        })
    )


def classify_trip_quality(df, pickup_column: str, dropoff_column: str):
    checks = _trip_checks(pickup_column, dropoff_column)
    checks["missing_trip_distance"] = "trip_distance IS NOT NULL"
    checks["rescued_data"] = "_rescued_data IS NULL"
    reasons = [F.when(~F.expr(condition), F.lit(reason)) for reason, condition in checks.items()]
    return df.withColumn(
        "rejection_reasons", F.filter(F.array(*reasons), lambda reason: reason.isNotNull())
    )
