import duckdb
import pytest
from pyspark.sql import SparkSession, functions as F

from trip_transforms import classify_trip_quality, prepare_bronze


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("nyc-taxi-quality-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_bronze_reads_file_lineage_from_nested_parquet(spark, tmp_path):
    month = tmp_path / "2026-01"
    month.mkdir()
    path = month / "yellow_tripdata_2026-01.parquet"
    with duckdb.connect() as con:
        con.sql("""
            SELECT TIMESTAMP '2026-02-01 10:00:00' AS tpep_pickup_datetime,
                   42::BIGINT AS customer_id, 'fixture' AS customer_email
        """).write_parquet(str(path))
    (month / "_publication.json").write_text("{}", encoding="utf-8")

    source = (
        spark.read.option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet").parquet(str(tmp_path))
        .withColumn("_rescued_data", F.lit(None).cast("string"))
    )
    bronze = prepare_bronze(source, "tpep_pickup_datetime")
    assert "customer_email" not in bronze.columns
    [row] = bronze.select(
        "customer_id", "source_file_name", "source_file_path", "source_file_size",
        "source_month", "year", "month", "day",
        F.col("source_file_modified_at").isNotNull().alias("has_modified_time"),
    ).collect()
    assert (row.customer_id, row.source_file_name) == (42, path.name)
    assert row.source_file_path.endswith("/2026-01/" + path.name)
    assert row.source_file_size == path.stat().st_size
    assert row.has_modified_time
    assert row.source_month == "2026-01"
    assert (row.year, row.month, row.day) == (2026, 2, 1)


@pytest.mark.parametrize("prefix", ["tpep", "lpep"])
def test_quality_routes_every_row_and_keeps_rescued_payload(spark, prefix):
    pickup, dropoff = f"{prefix}_pickup_datetime", f"{prefix}_dropoff_datetime"
    source = spark.sql(f"""
        SELECT row_id, timestamp_seconds(pickup_seconds) AS {pickup},
               timestamp_seconds(dropoff_seconds) AS {dropoff},
               pickup_id AS PULocationID, 20 AS DOLocationID,
               distance AS trip_distance, payload AS _rescued_data,
               'fixture.parquet' AS source_file_name,
               '2026-01' AS source_month
        FROM VALUES
            (1,    0, 90, 10,  0.0D, NULL),
            (2, NULL, 90,  0, -1.0D, NULL),
            (3,    0,  0, 10,  NULL, NULL),
            (4,    0, 90, 10,  1.0D, '{{"trip_distance":"bad"}}'),
            (5,    0, 90, 10,  1.0D, '{{"New_Column":"extra"}}')
        AS trips(row_id, pickup_seconds, dropoff_seconds, pickup_id, distance, payload)
    """)
    classified = classify_trip_quality(source, pickup, dropoff)
    accepted = classified.filter("size(rejection_reasons) = 0").collect()
    rejected = classified.filter("size(rejection_reasons) > 0").collect()

    assert [row.row_id for row in accepted] == [1]
    assert len(accepted) + len(rejected) == source.count() == 5
    by_id = {row.row_id: row for row in rejected}
    assert by_id[2].rejection_reasons == [
        "missing_pickup_timestamp", "invalid_pickup_location", "negative_trip_distance"
    ]
    assert by_id[3].rejection_reasons == ["invalid_trip_duration", "missing_trip_distance"]
    for row_id in (4, 5):
        assert by_id[row_id].rejection_reasons == ["rescued_data"]
        assert by_id[row_id]._rescued_data
        assert by_id[row_id].source_file_name == "fixture.parquet"
        assert by_id[row_id].source_month == "2026-01"
