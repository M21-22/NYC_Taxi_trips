import json

import duckdb
import pytest

from enrich_trip_data import enrich


def write_source(root, taxi_type, prefix, start="2026-01-05"):
    path = root / taxi_type / "trips.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.sql(f"""
            SELECT TIMESTAMP '{start} 10:00:00' AS {prefix}_pickup_datetime,
                   TIMESTAMP '{start} 10:10:00' AS {prefix}_dropoff_datetime,
                   10 AS PULocationID, 20 AS DOLocationID
            FROM range(2)
        """).write_parquet(str(path))


def test_enrichment_preserves_rows_and_assigns_stable_ids(tmp_path):
    for root_name in ("first", "second"):
        root = tmp_path / root_name
        write_source(root, "yellow", "tpep")
        write_source(root, "green", "lpep")

    first = enrich(tmp_path / "first", "v1", customer_count=10)
    second = enrich(tmp_path / "second", "v1", customer_count=10)
    assert enrich(tmp_path / "first", "v1", customer_count=10) == first

    manifest = json.loads((first / "manifest.json").read_text(encoding="utf-8"))
    pool = first.parents[1] / manifest["customer_pool"]

    with duckdb.connect() as con:
        assert con.execute(
            "SELECT count(*), count(DISTINCT customer_id) FROM read_parquet(?)",
            [str(pool)],
        ).fetchone() == (10, 10)

        for taxi_type in ("yellow", "green"):
            left = con.read_parquet(str(first / taxi_type / "trips.parquet"))
            right = con.read_parquet(str(second / taxi_type / "trips.parquet"))
            assert left.count("*").fetchone()[0] == 2
            assert {"customer_id", "PULocationID", "DOLocationID"} <= set(left.columns)
            assert not {"customer_first_name", "customer_last_name", "customer_email"} & set(left.columns)
            ids = left.project("customer_id").fetchall()
            assert ids == right.project("customer_id").fetchall()
            assert ids[0] == ids[1]
            assert all(1 <= row[0] <= 10 for row in ids)


def test_failed_batch_is_not_published(tmp_path):
    write_source(tmp_path, "yellow", "tpep")
    bad_source = tmp_path / "green" / "trips.parquet"
    bad_source.parent.mkdir()
    bad_source.write_bytes(b"Invalid Parquet fixture")

    with pytest.raises(duckdb.Error):
        enrich(tmp_path, "v1", customer_count=10)

    assert list((tmp_path / "enriched" / "batches").iterdir()) == []


def test_existing_pool_version_rejects_config_changes(tmp_path):
    write_source(tmp_path, "yellow", "tpep")
    enrich(tmp_path, "v1", customer_count=10)

    with pytest.raises(ValueError, match="new pool version"):
        enrich(tmp_path, "v1", customer_count=11)