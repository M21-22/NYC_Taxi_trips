import argparse
import csv
import tempfile
from pathlib import Path

import duckdb
from faker import Faker


def sql_string(value: Path) -> str:
    """Return a safely quoted DuckDB string literal for a local path."""
    return "'" + str(value.resolve()).replace("'", "''") + "'"


def create_customer_pool(path: Path, count: int, seed: int) -> None:
    fake = Faker("en_US")
    Faker.seed(seed)

    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=(
                "synthetic_customer_id",
                "customer_first_name",
                "customer_last_name",
                "customer_email",
            ),
        )
        writer.writeheader()

        for customer_id in range(1, count + 1):
            first_name = fake.first_name()
            last_name = fake.last_name()
            writer.writerow(
                {
                    "synthetic_customer_id": customer_id,
                    "customer_first_name": first_name,
                    "customer_last_name": last_name,
                    "customer_email": (
                        f"{first_name}.{last_name}.{customer_id}@example.com"
                    ).lower().replace(" ", "_").replace("'", ""),
                }
            )


def enrich_file(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
    destination: Path,
    customer_pool: Path,
    customer_count: int,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = destination.with_suffix(".tmp.parquet")

    query = f"""
        COPY (
            SELECT
                trips.*,
                customers.customer_first_name,
                customers.customer_last_name,
                customers.customer_email
            FROM read_parquet({sql_string(source)}) AS trips
            JOIN read_csv_auto(
                {sql_string(customer_pool)},
                header = TRUE,
                all_varchar = TRUE
            ) AS customers
              ON CAST(customers.synthetic_customer_id AS UBIGINT)
                 = 1 + (hash(to_json(trips)) % {customer_count})
        )
        TO {sql_string(temporary_output)}
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """

    try:
        connection.execute(query)
        temporary_output.replace(destination)
    finally:
        temporary_output.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add deterministic synthetic PII to yellow and green trips."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--customer-count", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.customer_count < 1:
        raise ValueError("--customer-count must be at least 1")

    source_files = [
        (taxi_type, source)
        for taxi_type in ("yellow", "green")
        for source in sorted((args.data_dir / taxi_type).glob("*.parquet"))
    ]
    if not source_files:
        raise FileNotFoundError(
            f"No Parquet files found under {args.data_dir / 'yellow'} or "
            f"{args.data_dir / 'green'}"
        )

    with tempfile.TemporaryDirectory() as temporary_directory:
        customer_pool = Path(temporary_directory) / "synthetic_customers.csv"
        create_customer_pool(customer_pool, args.customer_count, args.seed)

        with duckdb.connect() as connection:
            for taxi_type, source in source_files:
                destination = args.data_dir / "enriched" / taxi_type / source.name
                print(f"Enriching {source} -> {destination}")
                enrich_file(
                    connection,
                    source,
                    destination,
                    customer_pool,
                    args.customer_count,
                )

    print(f"Enriched {len(source_files)} Parquet files successfully.")


if __name__ == "__main__":
    main()
