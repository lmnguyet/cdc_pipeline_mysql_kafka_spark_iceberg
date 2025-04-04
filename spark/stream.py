import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from minio import Minio
from minio.error import S3Error

# create spark session
SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()

KAFKA_TOPIC = "dbserver1.pixar_films.films"

BUCKET_NAME = "pixarfilms"

DELTA_PATH = f"s3a://{BUCKET_NAME}/films"
CHECKPOINT_PATH = f"s3a://{BUCKET_NAME}/checkpoints"

MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minio",
    "secret_key": "minio123"
}

logging.basicConfig(
    filename='/app/stream.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# read stream from kafka
def read_stream():
    df = SPARK.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    return df

# process stream df
def process_stream(df):
    schema = StructType([
        StructField("payload", StructType([
            StructField("before", StructType([
                StructField("number", IntegerType(), nullable=False)
            ]))
            , StructField("after", StructType([
                StructField("number", IntegerType(), nullable=False),
                StructField("film", StringType(), nullable=False),
                StructField("release_date", IntegerType(), nullable=True),
                StructField("run_time", IntegerType(), nullable=True),
                StructField("film_rating", StringType(), nullable=True),
                StructField("plot", StringType(), nullable=True)
            ]))
            , StructField("op", StringType(), nullable=False)
            , StructField("ts_ms", IntegerType(), nullable=False)
        ]))
    ])

    parsed_df = df.select(f.from_json(f.col("value").cast("string"), schema).alias("parsed_value"))\
    .withColumn("release_date", f.date_add(f.lit("1970-01-01").cast("date"), f.col("parsed_value.payload.after.release_date")))

    processed_df = parsed_df\
    .select(
        f.when(f.col("parsed_value.payload.op") == "d", f.col("parsed_value.payload.before.number")).otherwise(f.col("parsed_value.payload.after.number")).cast("int").alias("number"),
        f.col("parsed_value.payload.after.film"),
        f.col("release_date"),
        f.col("parsed_value.payload.after.run_time").cast("int").alias("run_time"),
        f.col("parsed_value.payload.after.film_rating"),
        f.col("parsed_value.payload.after.plot"),
        f.col("parsed_value.payload.op"),
        f.col("parsed_value.payload.ts_ms").cast("int").alias("ts_ms")
    )

    logging.info(f"SCHEMA OF THE PROCESSED DATAFRAME \n{processed_df.schema} \n")

    return processed_df

# create bucket and delta table if not exist
def create_delta_table():
    try:
        client = Minio(endpoint=MINIO_CONFIG["endpoint"], access_key=MINIO_CONFIG["access_key"], secret_key=MINIO_CONFIG["secret_key"], secure=False)
        
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            logging.info(f"CREATING NEW BUCKET {BUCKET_NAME} \n")
        else:
            logging.info(f"BUCKET {BUCKET_NAME} ALREADY EXISTS \n")
            
    except S3Error as e:
        logging.error(f"MINIO ERROR WHILE CREATING NEW BUCKET {str(e)} \n")
        raise
    except Exception as e:
        logging.error(f"OTHER ERROR WHILE CREATING NEW BUCKET {str(e)} \n")
        raise

    if not DeltaTable.isDeltaTable(SPARK, DELTA_PATH):
        logging.info(f"CREATING NEW DELTA TABLE AT {DELTA_PATH} \n")
        SPARK.sql(f"""
            CREATE TABLE delta.`{DELTA_PATH}` (
                number INT,
                film STRING,
                release_date DATE,
                run_time INT,
                film_rating STRING,
                plot STRING
            )
            USING DELTA;
        """)
        logging.info(f"CREATED NEW DELTA TABLE AT {DELTA_PATH} \n")

# refresh query data from delta table
def refresh():
    refresh_count = 1
    while True:
        logging.info(f"NUMBER OF REFRESHING: {refresh_count} \n")

        if DeltaTable.isDeltaTable(SPARK, DELTA_PATH):
            df = SPARK.read.format("delta").load(DELTA_PATH)
            logging.info(f"NUMBER OF RECORDS: {df.count()} \n")
            logging.info("\n" + df._jdf.showString(40, 20, False) + "\n")
        else:
            logging.info(f"DELTA TABLE AT {DELTA_PATH} DOES NOT EXIST \n")
        
        refresh_count += 1

        time.sleep(30)

def upsertToDelta(microBatchOutputDF, batchId):
    window_spec = Window.partitionBy("number").orderBy(f.col("ts_ms").desc())

    processed_df = microBatchOutputDF.withColumn("row_num", f.row_number().over(window_spec))
    
    processed_df = processed_df.filter(f.col("row_num") == 1).drop("row_num", "ts_ms")

    upsert_df = processed_df.filter(f.col("op") != "d").drop("op")
    delete_df = processed_df.filter(f.col("op") == "d").drop("op")

    delta_table = DeltaTable.forPath(SPARK, DELTA_PATH)

    (delta_table.alias("target").merge(
        upsert_df.alias("source"),
        "source.number = target.number")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    (delta_table.alias("target").merge(
        delete_df.alias("source"),
        "source.number = target.number")
        .whenMatchedDelete()
        .execute()
    )

# write stream into delta table
def write_stream(processed_df):
    query = processed_df.writeStream\
    .foreachBatch(upsertToDelta)\
    .outputMode("update")\
    .option("checkpointLocation", CHECKPOINT_PATH)\
    .start()
    return query

def main():
    create_delta_table()

    df = read_stream()

    processed_df = process_stream(df)

    query = write_stream(processed_df)

    refresh()

    query.awaitTermination()

    SPARK.stop()
    

if __name__ == "__main__":
    main()