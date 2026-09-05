from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class PiiCrypto:
    def __init__(self, encryption_key: str, mode: str = "GCM", padding: str = "DEFAULT"):
        self.mode = mode
        self.padding = padding

        try:
            key_bytes = bytes.fromhex(encryption_key)
            is_hex_key = len(encryption_key) in (32, 48, 64)
        except ValueError:
            key_bytes = encryption_key.encode("utf-8")
            is_hex_key = False

        if len(key_bytes) not in (16, 24, 32):
            raise ValueError(f"AES key must be 16, 24, or 32 bytes. Current length: {len(key_bytes)}")

        escaped_key = encryption_key.replace("'", "''")
        self.key_expression = f"unhex('{escaped_key}')" if is_hex_key else f"'{escaped_key}'"

    def encrypt_columns(self, df: DataFrame, fields: list[str]) -> DataFrame:
        result_df = df

        for field in fields:
            if field not in result_df.columns:
                raise ValueError(f"Column not found: {field}")

            result_df = result_df.withColumn(
                field,
                F.when(
                    F.col(field).isNull(),
                    F.lit(None)
                ).otherwise(
                    F.base64(
                        F.expr(
                            f"aes_encrypt(CAST({field} AS STRING), "
                            f"{self.key_expression}, "
                            f"'{self.mode}', "
                            f"'{self.padding}')"
                        )
                    )
                )
            )

        return result_df

    def decrypt_columns(self, df: DataFrame, fields: list[str]) -> DataFrame:
        result_df = df

        for field in fields:
            if field not in result_df.columns:
                raise ValueError(f"Column not found: {field}")

            result_df = result_df.withColumn(
                field,
                F.when(
                    F.col(field).isNull(),
                    F.lit(None)
                ).otherwise(
                    F.expr(
                        f"CAST(aes_decrypt(unbase64({field}), "
                        f"{self.key_expression}, "
                        f"'{self.mode}', "
                        f"'{self.padding}') AS STRING)"
                    )
                )
            )

        return result_df