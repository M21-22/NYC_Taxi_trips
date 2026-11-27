import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from customer_pool import CUSTOMER_FIELDS
from encrypt_customers_job import encrypt_pool, read_customer_pool


def test_encrypts_uploaded_values_and_rejects_incomplete_pool(tmp_path):
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-encrypt-uploaded-customers")
        .getOrCreate()
    )
    try:
        rows = [(1, "Uploaded", "Record", "uploaded@example.invalid")]
        source = spark.sql("""
            SELECT CAST(1 AS BIGINT) AS customer_id,
                   'Uploaded' AS customer_first_name,
                   'Record' AS customer_last_name,
                   'uploaded@example.invalid' AS customer_email
        """)
        source.write.parquet(str(tmp_path / "customers.parquet"))

        metadata = tmp_path / "pool.json"
        metadata.write_text(json.dumps({"pool_version": "v1", "seed": 42, "count": 1}))
        customers, seed, count = read_customer_pool(spark, str(tmp_path), "v1")
        assert (seed, count) == (42, 1)

        key_hex = "ab" * 32
        encrypted = encrypt_pool(
            customers, pool_version="v1", seed=seed, key_id="k1", key_hex_sql=f"'{key_hex}'"
        )
        assert set(CUSTOMER_FIELDS).isdisjoint(encrypted.columns)
        restored = encrypted.select(
            "customer_id",
            *(
                F.aes_decrypt(
                    f"{field}_ciphertext", F.unhex(F.lit(key_hex)), F.lit("GCM")
                ).cast("string")
                for field in CUSTOMER_FIELDS
            ),
        ).collect()
        assert [tuple(row) for row in restored] == rows

        metadata.write_text(json.dumps({"pool_version": "v1", "seed": 42, "count": 2}))
        with pytest.raises(ValueError, match="unique IDs from 1 to count"):
            read_customer_pool(spark, str(tmp_path), "v1")
    finally:
        spark.stop()
