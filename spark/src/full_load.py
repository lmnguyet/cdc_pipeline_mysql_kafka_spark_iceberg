import time
import logging
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

BUCKET_NAME = "warehouse"

DELTA_PATH = f"s3a://{BUCKET_NAME}/films"
CHECKPOINT_PATH = f"s3a://{BUCKET_NAME}/checkpoints"

MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minio",
    "secret_key": "minio123"
}

MYSQL_CONFIG = {
    "host": "mysql",
    "port": "3306",
    "database": "pixar_films",
    "table": "films",
    "user": "lminhnguyet",
    "password": "123"
}

def read_from_mysql():
    df = SPARK.read \
    .format("jdbc") \
    .option("url", f"jdbc:mysql://{MYSQL_CONFIG["host"]}:{MYSQL_CONFIG["port"]}/{MYSQL_CONFIG["database"]}") \
    .option("dbtable", MYSQL_CONFIG["table"]) \
    .option("user", MYSQL_CONFIG["user"]) \
    .option("password", MYSQL_CONFIG["password"]) \
    .load()
    return df

def create_bucket():
    try:
        client = Minio(endpoint=MINIO_CONFIG["endpoint"], access_key=MINIO_CONFIG["access_key"], secret_key=MINIO_CONFIG["secret_key"], secure=False)
        
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"CREATING NEW BUCKET {BUCKET_NAME}")
        else:
            print(f"BUCKET {BUCKET_NAME} ALREADY EXISTS")  
    except S3Error as e:
        print(f"MINIO ERROR WHILE CREATING NEW BUCKET {str(e)}")
        raise
    except Exception as e:
        print(f"OTHER ERROR WHILE CREATING NEW BUCKET {str(e)}")
        raise

def full_load():
    pass

def main():
    create_bucket()
    print(SPARK.conf.get("spark.sql.catalog.spark_catalog"))
    print(SPARK.conf.get("spark.sql.catalog.spark_catalog.type"))
    print(SPARK.conf.get("spark.sql.catalog.spark_catalog.uri"))

    df = read_from_mysql()
    df.printSchema()
    df.show(40)

    # df.writeTo("spark_catalog.default.films") \
    # .createOrReplace()

    # df.write \
    # .format("iceberg") \
    # .mode("overwrite") \
    # .save("my_catalog.warehouse.films")

    SPARK.sql("""
        CREATE OR REPLACE TABLE spark_catalog.default.films (
            number INT,
            film STRING,
            release_date DATE,
            run_time INT,
            film_rating STRING,
            plot STRING
        ) USING iceberg
        LOCATION 's3a://warehouse/default/films'
        """)

# Then append data
    df.write.format("iceberg").mode("append").saveAsTable("spark_catalog.default.films")

    SPARK.sql("SELECT * FROM spark_catalog.default.films").show(40)

    SPARK.stop()
    

if __name__ == "__main__":
    main()