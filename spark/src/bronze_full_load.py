from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error

# create spark session
SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()

BUCKET_NAME = "pixarfilms"

WAREHOUSE_PATH = f"s3a://{BUCKET_NAME}"
DATABASE_NAME = "default"
TABLE_NAME = "films"

SINK_TABLE = f"{DATABASE_NAME}.{TABLE_NAME}"

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

    SPARK.sql(f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.{SINK_TABLE} (
            number INT,
            film STRING,
            release_date DATE,
            run_time INT,
            film_rating STRING,
            plot STRING
        ) USING iceberg
        LOCATION '{WAREHOUSE_PATH}/{TABLE_NAME}'
        """)

def main():
    create_bucket()

    df = read_from_mysql()

    df.printSchema()
    df.show(40)

    df.write.format("iceberg").mode("append").saveAsTable(f"spark_catalog.{SINK_TABLE}")

    SPARK.sql(f"SELECT * FROM spark_catalog.{SINK_TABLE}").show(40)

    SPARK.stop()
    
if __name__ == "__main__":
    main()