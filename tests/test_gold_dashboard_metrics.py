import json
from pathlib import Path

import pytest

from test_trip_transforms import spark
from trip_transforms import aggregate_daily_route_metrics


def test_dashboard_uses_additive_gold_metrics(spark):
    trips = spark.sql("""
        SELECT 2026 AS year, 1 AS month, 'yellow' AS taxi_type,
               pickup AS PULocationID, 20 AS DOLocationID, trips.*
        FROM VALUES
            (5, 10, 1,  8.004D, 2.0D,  10.004D,  2.0D, 10.0D),
            (5, 10, 2,  NULL,   NULL,  NULL,     20.0D, NULL),
            (6, 11, 1, 90.0D,   NULL,  90.004D,  3.0D, 20.0D),
            (6, 98, 3,  NULL,   NULL,  NULL,      NULL, NULL),
            (6, 99, 1, 20.0D,   5.0D,  25.004D,  5.0D,  5.0D)
        AS trips(day, pickup, payment_type, fare_amount, tip_amount, total_amount,
                 trip_distance, trip_duration_minutes)
    """)
    zones = spark.sql("""
        SELECT * FROM VALUES
            (10, 'Manhattan', 'Shared name'),
            (11, 'Manhattan', 'Shared name'),
            (20, 'Brooklyn', 'Drop-off')
        AS zones(LocationID, Borough, Zone)
    """)
    gold = aggregate_daily_route_metrics(trips, zones)
    gold.createOrReplaceTempView("gold_dashboard_test")

    path = Path(__file__).resolve().parents[1] / "dashboards"
    dashboard = json.loads(next(path.glob("*.lvdash.json")).read_text(encoding="utf-8"))
    dataset = next(d for d in dashboard["datasets"] if d["displayName"] == "taxi_metrics")
    query, separator, _ = "".join(dataset["queryLines"]).rpartition("FROM ")
    assert separator
    spark.sql(query + "FROM gold_dashboard_test").createOrReplaceTempView("dashboard_test")

    routes = spark.table("dashboard_test").select("route").distinct().collect()
    assert len(routes) == 4
    assert {row.route.split(" → ")[0] for row in routes} == {
        "Shared name [10]", "Shared name [11]", "Unknown [98]", "Unknown [99]"
    }

    measures = {c["displayName"]: c["expression"] for c in dataset["columns"]}
    projection = ", ".join(f"{expr} AS `{name}`" for name, expr in measures.items())
    [summary] = spark.sql(
        f"SELECT SUM(total_passenger_charges) AS charges, {projection} FROM dashboard_test"
    ).collect()
    assert str(summary.charges) == "125.0120"
    for name, expected in {
        "Charges per trip": 125.012 / 3,
        "Tips per trip": 7 / 2,
        "Card share": 3 / 5,
        "Average distance": 30 / 4,
        "Average duration": 35 / 3,
        "Charges per mile": 125.012 / 10,
        "Recorded card tip rate": 7 / 28.004,
    }.items():
        assert float(summary[name]) == pytest.approx(expected, abs=0.000001)

    [unknown] = spark.sql(
        f"SELECT {projection} FROM dashboard_test WHERE pickup_zone_id = 98"
    ).collect()
    assert unknown["Charges per mile"] is None
    assert unknown["Average duration"] is None
    assert unknown["Recorded card tip rate"] is None
