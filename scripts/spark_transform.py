import argparse
import os
import clickhouse_connect
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import (
    LongType,
    MapType,
    StringType,
    StructType,
    StructField,
    DecimalType,
)

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

CH_HOST = os.getenv("CLICKHOUSE_HOST")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT"))
CH_USER = os.getenv("CLICKHOUSE_USER")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CH_DB = os.getenv("CLICKHOUSE_DB")
SILVER_TABLE_NAME = os.getenv("SILVER_TABLE_NAME")
BATCH_SIZE = 5000

def create_spark_session():
    return (
        SparkSession.builder.appName("CryptoBronzeToSilver")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled", "false"
        )
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

def insert_partition_to_ch(partition_iterator):
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )

    batch = []
    batch_size = BATCH_SIZE

    for row in partition_iterator:
        batch.append(
            (
                row["coin_id"],
                row["price_usd"] if row["price_usd"] is not None else Decimal("0.0"),
                row["market_cap_usd"] if row["market_cap_usd"] is not None else Decimal("0.0"),
                row["last_updated_at"],
                row["extracted_at"]
            )
        )

        if len(batch) >= batch_size:
            client.insert(
                table=SILVER_TABLE_NAME,
                data=batch,
                column_names=[
                    "coin_id",
                    "price_usd",
                    "market_cap_usd",
                    "last_updated_at",
                    "extracted_at"
                ]
            )
            batch = []

    if batch:
        client.insert(
            table=SILVER_TABLE_NAME,
            data=batch,
            column_names=[
                "coin_id",
                "price_usd",
                "market_cap_usd",
                "last_updated_at",
                "extracted_at"
            ]
        )

    client.close()

def process_crypto_data(target_date: str):
    spark = create_spark_session()
    print("Spark session created")

    coin_schema = StructType(
        [
            StructField("usd", DecimalType(18, 8), True),
            StructField("usd_market_cap", DecimalType(20, 2), True),
            StructField("last_updated_at", LongType(), True),
        ]
    )

    data_schema = StructType(
        [
            StructField("extracted_at", StringType(), True),
            StructField("data", MapType(StringType(), coin_schema), True),
        ]
    )

    dt = datetime.strptime(target_date, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    s3_path = f"s3a://{BUCKET_NAME}/year={year}/month={month}/day={day}/*.json"
    print(f"Reading JSON files from: {s3_path}")

    try:
        raw_df = spark.read.schema(data_schema).json(s3_path)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        spark.stop()
        return

    if raw_df.rdd.isEmpty():
        print(f"No data in bucket: {BUCKET_NAME}")
        spark.stop()
        return

    exploded_df = raw_df.select(
        F.to_timestamp(F.col("extracted_at")).alias("extracted_at"),
        F.explode(F.col("data")).alias("coin_id", "metrics")
    )

    flattened_df = (
        exploded_df.select(
            F.col("coin_id"),
            F.col("metrics.usd").alias("price_usd"),
            F.col("metrics.usd_market_cap").alias("market_cap_usd"),
            F.to_timestamp(
                F.from_unixtime(F.col("metrics.last_updated_at"))
            ).alias("last_updated_at"),
            F.col("extracted_at")
        )
        .where(F.col("last_updated_at").isNotNull())
        .dropDuplicates(["coin_id", "last_updated_at"])
    )

    print("Transformed data:")
    flattened_df.show(5, truncate=False)
    print("...")

    flattened_df.rdd.foreachPartition(insert_partition_to_ch)

    print("Data loaded into CH")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Date partition to process in YYYY-MM-DD format",
    )
    args = parser.parse_args()

    process_crypto_data(target_date=args.date)