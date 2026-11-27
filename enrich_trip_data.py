import argparse
import csv
import hashlib
import json
import re
import tempfile
from pathlib import Path

import duckdb
from customer_pool import CUSTOMER_FIELDS, customer_rows


TAXI_PREFIXES = {"yellow": "tpep", "green": "lpep"}
ASSIGNMENT_VERSION = "md5-taxi-times-zones-v1"


def file_digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def write_json(path, data):
    path.write_text(
        json.dumps(data, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def get_customer_pool(con, output_root, pool_version, seed, customer_count):
    destination = output_root / "customer_pools" / pool_version
    metadata = {"pool_version": pool_version, "seed": seed, "count": customer_count}
    destination.parent.mkdir(parents=True, exist_ok=True)

    if not destination.exists():
        with tempfile.TemporaryDirectory(
            prefix=".pool-", dir=destination.parent
        ) as temporary:
            stage = Path(temporary) / "publish"
            stage.mkdir()
            csv_path = Path(temporary) / "customers.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(("customer_id", *CUSTOMER_FIELDS))
                writer.writerows(customer_rows(seed, customer_count))

            con.sql(
                """
                SELECT * FROM read_csv(
                    ?, header = true,
                    columns = {
                        'customer_id': 'BIGINT',
                        'customer_first_name': 'VARCHAR',
                        'customer_last_name': 'VARCHAR',
                        'customer_email': 'VARCHAR'
                    }
                )
                """,
                params=[str(csv_path)],
            ).write_parquet(str(stage / "customers.parquet"), compression="zstd")
            validate_customer_pool(con, stage / "customers.parquet", customer_count)
            write_json(stage / "pool.json", metadata)

            try:
                stage.rename(destination)
            except OSError:
                if not destination.is_dir():
                    raise

    saved = json.loads((destination / "pool.json").read_text(encoding="utf-8"))
    if saved != metadata:
        raise ValueError("Use a new pool version when changing the seed or customer count.")

    pool = destination / "customers.parquet"
    validate_customer_pool(con, pool, customer_count)
    return pool


def validate_customer_pool(con, pool, customer_count):
    stats = con.execute(
        """
        SELECT count(*), count(DISTINCT customer_id),
               min(customer_id), max(customer_id)
        FROM read_parquet(?)
        """,
        [str(pool)],
    ).fetchone()
    if stats != (customer_count, customer_count, 1, customer_count):
        raise RuntimeError("Customer pool IDs must be unique and cover 1 through customer_count.")


def write_enriched_trips(con, source, target, taxi_type, pool, customer_count):
    prefix = TAXI_PREFIXES[taxi_type]
    source_relation = con.read_parquet(str(source))
    columns = {column.lower() for column in source_relation.columns}
    if columns.intersection(CUSTOMER_FIELDS, {"customer_id", "synthetic_customer_id"}):
        raise ValueError(f"Expected original TLC input: {source}")

    target.parent.mkdir(parents=True, exist_ok=True)
    con.sql(
        f"""
        SELECT trips.*,
               CAST(1 + md5_number_lower(to_json(struct_pack(
                   taxi_type := CAST(? AS VARCHAR),
                   pickup := CAST({prefix}_pickup_datetime AS VARCHAR),
                   dropoff := CAST({prefix}_dropoff_datetime AS VARCHAR),
                   pickup_zone := CAST(PULocationID AS BIGINT),
                   dropoff_zone := CAST(DOLocationID AS BIGINT)
               ))) % CAST(? AS UBIGINT) AS BIGINT) AS customer_id
        FROM read_parquet(?) AS trips
        """,
        params=[taxi_type, customer_count, str(source)],
    ).write_parquet(str(target), compression="zstd")

    source_count = source_relation.count("*").fetchone()[0]
    output_count, unresolved = con.execute(
        """
        SELECT count(*), count(*) FILTER (WHERE customers.customer_id IS NULL)
        FROM read_parquet(?) AS trips
        LEFT JOIN read_parquet(?) AS customers USING (customer_id)
        """,
        [str(target), str(pool)],
    ).fetchone()
    if output_count != source_count or unresolved:
        raise RuntimeError(f"Trip count or customer ID validation failed: {source}")
    return output_count


def enrich(data_dir, pool_version, seed=42, customer_count=10_000,
           memory_limit="1GB", threads=2, temp_directory=None):
    if not re.fullmatch(r"v[1-9][0-9]*", pool_version):
        raise ValueError("Pool version must have the form v1, v2, and so on.")
    if not 1 <= customer_count <= 2**63 - 1 or threads < 1:
        raise ValueError("Invalid customer count or thread count.")

    data_dir = Path(data_dir).resolve()
    sources = [
        (taxi_type, path)
        for taxi_type in TAXI_PREFIXES
        for path in sorted((data_dir / taxi_type).glob("*.parquet"))
    ]
    if not sources:
        raise FileNotFoundError("No Parquet files found in the yellow or green input folders.")

    inputs = [
        {"path": path.relative_to(data_dir).as_posix(), "sha256": file_digest(path)}
        for _, path in sources
    ]
    identity = {
        "pool_version": pool_version,
        "seed": seed,
        "customer_count": customer_count,
        "assignment_version": ASSIGNMENT_VERSION,
        "inputs": inputs,
    }
    batch_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True).encode("utf-8")
    ).hexdigest()

    output_root = data_dir / "enriched"
    destination = output_root / "batches" / batch_id
    destination.parent.mkdir(parents=True, exist_ok=True)
    if temp_directory is not None:
        temp_directory = Path(temp_directory).resolve()
        temp_directory.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix=".batch-", dir=destination.parent
    ) as temporary:
        stage = Path(temporary) / "publish"
        stage.mkdir()
        spill = temp_directory or Path(temporary) / "spill"

        with duckdb.connect(config={
            "memory_limit": memory_limit,
            "threads": str(threads),
            "temp_directory": str(spill),
        }) as con:
            con.execute("SET TimeZone = 'UTC'")
            pool = get_customer_pool(
                con, output_root, pool_version, seed, customer_count
            )
            if (destination / "manifest.json").is_file():
                return destination

            outputs = []
            for (taxi_type, source), entry in zip(sources, inputs, strict=True):
                relative = Path(taxi_type) / source.name
                rows = write_enriched_trips(
                    con, source, stage / relative, taxi_type, pool, customer_count
                )
                if file_digest(source) != entry["sha256"]:
                    raise RuntimeError(f"Input changed during enrichment: {source}")
                outputs.append({"path": relative.as_posix(), "rows": rows})

        write_json(stage / "manifest.json", {
            **identity,
            "customer_pool": pool.relative_to(output_root).as_posix(),
            "outputs": outputs,
        })
        try:
            stage.rename(destination)
        except OSError:
            if not (destination / "manifest.json").is_file():
                raise

    return destination


def main():
    parser = argparse.ArgumentParser(
        description="Assign synthetic customer IDs to local TLC trip files."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--pool-version", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--customer-count", type=int, default=10_000)
    parser.add_argument("--memory-limit", default="1GB")
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--temp-directory", type=Path)
    print(enrich(**vars(parser.parse_args())))


if __name__ == "__main__":
    main()