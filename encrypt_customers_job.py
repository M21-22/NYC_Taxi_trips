"""Encrypt an enriched customer Parquet pool staged in Unity Catalog."""

import argparse
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_pool import CUSTOMER_FIELDS


def read_customer_pool(
    spark: SparkSession, path: str, pool_version: str
) -> tuple[DataFrame, int, int]:
    metadata = (
        spark.read.schema("pool_version STRING, seed BIGINT, count BIGINT")
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(f"{path}/pool.json")
        .take(2)
    )
    if len(metadata) != 1:
        raise ValueError("pool.json must describe exactly one customer pool.")
    version, seed, count = metadata[0]
    if version != pool_version or seed is None or count is None or count < 1:
        raise ValueError(
            "pool.json must match the requested version and contain a seed and positive count."
        )

    customers = spark.read.parquet(f"{path}/customers.parquet")
    expected_types = {"customer_id": "bigint", **dict.fromkeys(CUSTOMER_FIELDS, "string")}
    actual_types = dict(customers.dtypes)
    if any(actual_types.get(column) != kind for column, kind in expected_types.items()):
        raise ValueError(
            "customers.parquet must have a BIGINT customer_id and three STRING customer fields."
        )

    customers = customers.select(*expected_types)
    stats = customers.agg(
        F.count("*"),
        F.countDistinct("customer_id"),
        F.min("customer_id"),
        F.max("customer_id"),
        *(F.count(field) for field in CUSTOMER_FIELDS),
    ).first()
    if tuple(stats) != (count, count, 1, count, *(count for _ in CUSTOMER_FIELDS)):
        raise ValueError(
            "The uploaded pool must contain unique IDs from 1 to count and non-null customer fields."
        )
    return customers, seed, count


def encrypt_pool(
    customers: DataFrame, *, pool_version: str, seed: int, key_id: str, key_hex_sql: str
) -> DataFrame:
    key_hex = F.expr(key_hex_sql)
    key = F.when(key_hex.rlike(r"^[0-9a-fA-F]{64}$"), F.unhex(key_hex)).otherwise(
        F.raise_error(F.lit("The customer AES key must contain exactly 64 hex characters"))
    )
    return customers.select(
        F.lit(pool_version).alias("pool_version"),
        F.col("customer_id"),
        F.lit(seed).cast("bigint").alias("pool_seed"),
        F.lit(key_id).alias("key_id"),
        *(
            F.aes_encrypt(F.encode(field, "UTF-8"), key, F.lit("GCM")).alias(
                f"{field}_ciphertext"
            )
            for field in CUSTOMER_FIELDS
        ),
    )


def main() -> None:
    from databricks.connect import DatabricksSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pool-version", required=True)
    parser.add_argument("--key-id", required=True)
    args = parser.parse_args()

    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.catalog):
        parser.error("--catalog must be a lowercase SQL identifier")
    if not re.fullmatch(r"v[1-9][0-9]*", args.pool_version):
        parser.error("--pool-version must have the form v1, v2, and so on")
    if not re.fullmatch(r"k[1-9][0-9]*", args.key_id):
        parser.error("--key-id must have the form k1, k2, and so on")
    spark = DatabricksSession.builder.getOrCreate()
    source_path = f"/Volumes/{args.catalog}/restricted/customers/incoming/{args.pool_version}"
    customers, seed, count = read_customer_pool(spark, source_path, args.pool_version)
    table = f"`{args.catalog}`.`restricted`.`customers_encrypted`"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            pool_version STRING NOT NULL,
            customer_id BIGINT NOT NULL,
            pool_seed BIGINT NOT NULL,
            key_id STRING NOT NULL,
            customer_first_name_ciphertext BINARY NOT NULL,
            customer_last_name_ciphertext BINARY NOT NULL,
            customer_email_ciphertext BINARY NOT NULL
        ) USING DELTA
    """).collect()

    existing = (
        spark.table(table)
        .where(F.col("pool_version") == args.pool_version)
        .groupBy("pool_seed")
        .count()
        .collect()
    )
    if existing and [(row.pool_seed, row["count"]) for row in existing] != [
        (seed, count)
    ]:
        raise ValueError("Use a new pool version when changing the seed or customer count.")

    # Only a secret reference enters the Spark plan.
    encrypted = encrypt_pool(
        customers,
        pool_version=args.pool_version,
        seed=seed,
        key_id=args.key_id,
        key_hex_sql=f"secret('{args.catalog}-pii', 'aes-{args.key_id}')",
    )
    encrypted.createOrReplaceTempView("encrypted_customer_pool")
    spark.sql(
        f"""
        INSERT INTO {table} REPLACE WHERE pool_version = :pool_version
        SELECT pool_version, customer_id, pool_seed, key_id,
               customer_first_name_ciphertext, customer_last_name_ciphertext,
               customer_email_ciphertext
        FROM encrypted_customer_pool
        """,
        args={"pool_version": args.pool_version},
    ).collect()
