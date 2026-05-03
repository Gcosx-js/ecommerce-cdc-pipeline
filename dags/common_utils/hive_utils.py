from pyspark.sql import SparkSession
from common_utils.config import MINIO_WAREHOUSE


def create_database(spark: SparkSession, db: str) -> None:
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db}
        LOCATION '{MINIO_WAREHOUSE}/databases/{db}'
    """)


def create_external_table(spark: SparkSession, db: str, table: str, columns: str, location: str) -> None:
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} (
            {columns}
        )
        PARTITIONED BY (date DATE)
        STORED AS PARQUET
        LOCATION '{location}'
    """)


def repair_table(spark: SparkSession, db: str, table: str) -> None:
    spark.sql(f"MSCK REPAIR TABLE {db}.{table}")