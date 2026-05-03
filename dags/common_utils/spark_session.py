from pyspark.sql import SparkSession
from common_utils.config import (
    SPARK_MASTER, SPARK_JARS_PATH,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    HIVE_METASTORE_URI,
)


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
        .config("spark.executor.extraClassPath", "/opt/spark/jars/extra/*")
        .config("hive.metastore.uris", HIVE_METASTORE_URI)
        .enableHiveSupport()
        .getOrCreate()
    )