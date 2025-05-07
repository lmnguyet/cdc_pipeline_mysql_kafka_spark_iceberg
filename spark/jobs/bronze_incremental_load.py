from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window
from minio import Minio
from minio.error import S3Error
import pytz
import argparse
import json
import os

# create spark session
SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()

# source config
SOURCE_BUCKET_NAME = "incpixarfilms"
TOPIC_PREFIX = "dbserver1.pixar_films"

UPSERT_DATE = datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime('%Y-%m-%d')

def load_params():
    """Load params from params file"""
    global WAREHOUSE_PATH, DATABASE_NAME, TABLE_KEYS
    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, 'params.json')
    try:
        with open(params_path) as f:
            params = json.load(f)
        WAREHOUSE_PATH = params["warehouse_path"]
        DATABASE_NAME = params["database"]
        TABLE_KEYS = params["table_keys"]
        
        print(f"LOADED PARAMS SUCCESSFULLY.")

    except FileNotFoundError:
        raise ValueError(f"No processing logic found")

def read_upsert(src_table, sink_table, key_columns=["number"]):
    sink_df = SPARK.sql(f"SELECT * FROM {DATABASE_NAME}.{sink_table}")
    sink_schema = sink_df.schema

    # read source to merge
    source_path = f"s3a://{SOURCE_BUCKET_NAME}/topics/{TOPIC_PREFIX}.{src_table}/{UPSERT_DATE}/*.json"
    try:
        df = SPARK.read.format("json").load(source_path)
    except Exception as e:
        return None

    # in case there're only insert/delete records in upsert df, json will parse them as string type
    for col in ["after", "before"]:
        if not isinstance(df.schema["payload"].dataType[col].dataType, StructType):
            df = df.withColumn("payload", 
                f.col("payload").withField(col, f.from_json(f.col(f"payload.{col}"), sink_schema))
            )

    # select columns
    selected_columns = []

    for field in sink_schema.fields:
        col_name = field.name
        col_type = field.dataType

        if col_name == "sequence_number":
            selected_columns.append(f.col("payload.source.ts_ms").cast(col_type).alias(col_name))
        elif col_name in key_columns:
            selected_columns.append(
                    f.when(
                        f.col("payload.op") == "d", f.col(f"payload.before.{col_name}")
                    ).otherwise(f.col(f"payload.after.{col_name}"))\
                    .cast(col_type).alias(col_name))
        else:
            if isinstance(col_type, DateType):
                selected_columns.append(
                    f.date_add(
                        f.lit("1970-01-01").cast("date"), f.col(f"payload.after.{col_name}").cast("int")
                    ).cast(col_type).alias(col_name)
                )
            else:
                selected_columns.append(f.col(f"payload.after.{col_name}").cast(col_type).alias(col_name))

    selected_columns.append(f.col("payload.op").alias("op"))

    selected_df = df.select(*selected_columns)

    # sort by sequence_number to get latest record
    window_spec = Window.partitionBy(key_columns).orderBy(f.col("sequence_number").desc())

    processed_df = selected_df.withColumn("row_num", f.row_number().over(window_spec))
    
    processed_df = processed_df.filter(f.col("row_num") == 1).drop("row_num")

    return processed_df

def merge(upsert_df, sink_table, key_columns=["number"]):
    upsert_df.createOrReplaceTempView("upserts")

    join_str = " AND ".join([f"target.{key} = source.{key}" for key in key_columns])

    SPARK.sql(f"""
        MERGE INTO {DATABASE_NAME}.{sink_table} target
        USING upserts source
        ON {join_str}
        WHEN MATCHED AND source.op = 'd' AND source.sequence_number > target.sequence_number THEN DELETE
        WHEN MATCHED AND source.op <> 'd' AND source.sequence_number > target.sequence_number THEN UPDATE SET *
        WHEN NOT MATCHED AND source.op <> 'd' THEN INSERT *
    """)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tables",
        default="brz_films,brz_film_ratings,brz_genres,brz_box_office",
        help="Comma-separated list of tables to process"
    )
    args = parser.parse_args()

    load_params()

    table_names = args.tables.split(',')
    topic_names = [table[4:] for table in table_names]

    for topic, table in zip(topic_names, table_names):
        print(f"===== MERGING TABLE {table} =====")
        processed_df = read_upsert(topic, table, TABLE_KEYS[table])

        if processed_df is None:
            print(f"===== NOTHING TO MERGE ON TABLE {table}, SKIPPED =====")
            continue

        processed_df.printSchema()

        merge(processed_df, table, TABLE_KEYS[table])

        print(f"===== FINISHED MERGING TABLE {table} =====")

    SPARK.stop()

if __name__ == "__main__":
    main()