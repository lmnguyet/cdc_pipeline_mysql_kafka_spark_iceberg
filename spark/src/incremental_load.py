import time
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window
from minio import Minio
from minio.error import S3Error

# create spark session
SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()

SINK_BUCKET_NAME = "pixarfilms"
SOURCE_BUCKET_NAME = "incpixarfilms"
TOPIC_NAME = "dbserver1.pixar_films.films"

WAREHOUSE_PATH = f"s3a://{SINK_BUCKET_NAME}"
DATABASE_NAME = "default"
TABLE_NAME = "films"

SINK_TABLE = f"{DATABASE_NAME}.{TABLE_NAME}"

UPSERT_DATE = datetime.now().strftime('%Y-%m-%d')
SOURCE_PATH = f"s3a://{SOURCE_BUCKET_NAME}/topics/{TOPIC_NAME}/{UPSERT_DATE}/*.json"

def read_upsert():
    df = SPARK.read.json(SOURCE_PATH)
    df = df.select(
        f.when(f.col("payload.op") == "d", f.col("payload.before.number")).otherwise(f.col("payload.after.number")).cast("int").alias("number"),
        f.col("payload.after.film"),
        f.date_add(f.lit("1970-01-01").cast("date"), f.col("payload.after.release_date").cast("int")).alias("release_date"),
        f.col("payload.after.run_time").cast("int").alias("run_time"),
        f.col("payload.after.film_rating"),
        f.col("payload.after.plot"),
        f.col("payload.op"),
        f.col("payload.ts_ms")
    )

    window_spec = Window.partitionBy("number").orderBy(f.col("ts_ms").desc())

    processed_df = df.withColumn("row_num", f.row_number().over(window_spec))
    
    processed_df = processed_df.filter(f.col("row_num") == 1).drop("row_num", "ts_ms")

    return processed_df

def merge(upsert_df, key_column="number"):
    upsert_df.createOrReplaceTempView("upserts")

    SPARK.sql(f"""
        MERGE INTO spark_catalog.{SINK_TABLE} target
        USING (
            SELECT number, film, release_date, run_time, film_rating, plot, op
            FROM upserts
        ) source
        ON target.{key_column} = source.{key_column}
        WHEN MATCHED AND source.op = 'd' THEN DELETE
        WHEN MATCHED AND source.op <> 'd' THEN UPDATE SET *
        WHEN NOT MATCHED AND source.op <> 'd' THEN INSERT *
    """)

def main():
    processed_df = read_upsert()

    SPARK.sql(f"SELECT * FROM spark_catalog.{SINK_TABLE}").show(40)

    merge(processed_df)

    SPARK.sql(f"SELECT * FROM spark_catalog.{SINK_TABLE}").show(40)

    SPARK.stop()

if __name__ == "__main__":
    main()