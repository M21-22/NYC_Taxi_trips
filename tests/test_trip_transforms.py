from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from trip_transforms import (
    aggregate_daily_route_metrics,
    normalize_trips,
    valid_trip_condition,
)

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("nyc-taxi-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.parametrize("taxi_type,prefix", [("yellow", "tpep"), ("green", "lpep")])
def test_normalize_trips(spark, taxi_type, prefix):
    pickup_col = f"{prefix}_pickup_datetime"
    dropoff_col = f"{prefix}_dropoff_datetime"
    source = spark.sql(f"""
        SELECT 2026 AS year, 1 AS month, 5 AS day,
               TIMESTAMP '2026-01-05 10:00:00' AS {pickup_col},
               TIMESTAMP '2026-01-05 10:01:30' AS {dropoff_col},
               10 AS PULocationID, 20 AS DOLocationID,
               1 AS passenger_count, 0.5D AS trip_distance,
               1 AS payment_type, 8.0D AS fare_amount, 2.0D AS tip_amount,
               0.0D AS tolls_amount, 10.0D AS total_amount,
               'synthetic@example.com' AS customer_email
    """)

    result = normalize_trips(source, taxi_type, pickup_col, dropoff_col)
    [row] = result.collect()

    assert row.taxi_type == taxi_type
    assert (row.PULocationID, row.DOLocationID) == (10, 20)
    assert row.trip_duration_minutes == pytest.approx(1.5)
    assert row.total_amount == 10.0
    assert {pickup_col, dropoff_col, "customer_email"}.isdisjoint(result.columns)
    assert {"pickup_datetime", "dropoff_datetime"}.issubset(result.columns)


@pytest.mark.parametrize("prefix", ["tpep", "lpep"])
def test_silver_trip_condition(spark, prefix):
    pickup_col = f"{prefix}_pickup_datetime"
    dropoff_col = f"{prefix}_dropoff_datetime"

    source = spark.sql(f"""
        SELECT trip_case,
               timestamp_seconds(pickup_seconds) AS {pickup_col},
               timestamp_seconds(dropoff_seconds) AS {dropoff_col},
               pickup_id AS PULocationID,
               dropoff_id AS DOLocationID,
               distance AS trip_distance
        FROM VALUES
            ('valid',               0,   90,   10,   20,  0.5D),
            ('zero_distance',       0,   90,   10,   20,  0.0D),
            ('no_pickup',        NULL,   90,   10,   20,  0.5D),
            ('no_dropoff',          0, NULL,   10,   20,  0.5D),
            ('reversed_times',     90,    0,   10,   20,  0.5D),
            ('equal_times',         0,    0,   10,   20,  0.5D),
            ('no_pickup_zone',      0,   90, NULL,   20,  0.5D),
            ('no_dropoff_zone',     0,   90,   10, NULL,  0.5D),
            ('bad_pickup_zone',     0,   90,    0,   20,  0.5D),
            ('bad_dropoff_zone',    0,   90,   10,   -1,  0.5D),
            ('negative_distance',   0,   90,   10,   20, -0.5D),
            ('null_distance',       0,   90,   10,   20,  NULL)
        AS trips(trip_case, pickup_seconds, dropoff_seconds,
                 pickup_id, dropoff_id, distance)
    """)

    accepted = source.filter(valid_trip_condition(pickup_col, dropoff_col))
    assert {row.trip_case for row in accepted.select("trip_case").collect()} == {
        "valid",
        "zero_distance",
    }


def test_aggregate_daily_route_metrics(spark):
    trips = spark.sql("""
        SELECT 2026 AS year, 1 AS month, trips.*
        FROM VALUES
            (5, 'yellow', 10, 20, 1,  8.0D, 2.0D, 10.0D, 2.0D, 10.0D),
            (5, 'yellow', 10, 20, 2, 12.0D, 0.0D, 12.0D, 3.0D, 20.0D),
            (5, 'yellow', 10, 20, 3,  0.0D, 0.0D,  0.0D, 1.0D,  6.0D),
            (6, 'yellow', 10, 20, 2,  6.0D, 0.0D,  6.0D, 1.0D,  5.0D),
            (5, 'green',  10, 20, 3,  0.0D, 0.0D,  0.0D, 0.0D,  4.0D),
            (5, 'yellow', 99, 20, 2,  5.0D, 0.0D,  5.0D, 1.0D,  3.0D)
        AS trips(day, taxi_type, PULocationID, DOLocationID, payment_type,
                 fare_amount, tip_amount, total_amount, trip_distance,
                 trip_duration_minutes)
    """)
    zones = spark.sql("""
        SELECT * FROM VALUES
            (10, 'Manhattan', 'Pickup zone'),
            (20, 'Brooklyn', 'Dropoff zone')
        AS zones(LocationID, Borough, Zone)
    """)

    result = aggregate_daily_route_metrics(trips, zones).collect()
    by_route = {
        (row.day, row.taxi_type, row.pickup_zone, row.dropoff_zone): row
        for row in result
    }
    assert len(result) == len(by_route) == 4

    yellow = by_route[(5, "yellow", "Pickup zone", "Dropoff zone")]
    assert (yellow.pickup_borough, yellow.dropoff_borough) == ("Manhattan", "Brooklyn")
    assert (yellow.trip_count, yellow.credit_card_trip_count, yellow.cash_trip_count) == (3, 1, 1)
    assert str(yellow.total_passenger_charges) == "22.0000"
    assert str(yellow.total_tips) == "2.0000"
    assert yellow.total_trip_distance == pytest.approx(6.0)
    assert yellow.total_trip_duration_minutes == pytest.approx(36.0)
    assert yellow.duration_trip_count == 3
    assert str(yellow.credit_card_fare_total) == "8.0000"
    assert str(yellow.credit_card_tip_total) == "2.0000"

    assert by_route[(6, "yellow", "Pickup zone", "Dropoff zone")].trip_count == 1
    assert by_route[(5, "yellow", None, "Dropoff zone")].trip_count == 1

    green = by_route[(5, "green", "Pickup zone", "Dropoff zone")]
    assert green.credit_card_fare_total is None
    assert green.credit_card_tip_total is None
    assert green.charge_distance == 0
    assert green.credit_card_trip_count == green.cash_trip_count == 0


def test_gold_keeps_distinct_zone_ids(spark):
    trips = spark.sql("""
        SELECT 2026 AS year, 1 AS month, 5 AS day, 'yellow' AS taxi_type,
               pickup_id AS PULocationID, 20 AS DOLocationID,
               1 AS payment_type, 8.0D AS fare_amount, 2.0D AS tip_amount,
               10.0D AS total_amount, 2.0D AS trip_distance,
               10.0D AS trip_duration_minutes
        FROM VALUES (10), (11), (98), (99) AS trips(pickup_id)
    """)
    zones = spark.sql("""
        SELECT * FROM VALUES
            (10, 'Manhattan', 'Same name'),
            (11, 'Manhattan', 'Same name'),
            (20, 'Brooklyn', 'Drop-off')
        AS zones(LocationID, Borough, Zone)
    """)

    rows = aggregate_daily_route_metrics(trips, zones).collect()

    assert len(rows) == 4
    assert {(row.PULocationID, row.DOLocationID, row.trip_count) for row in rows} == {
        (10, 20, 1), (11, 20, 1), (98, 20, 1), (99, 20, 1),
    }