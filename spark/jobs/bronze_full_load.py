from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from minio import Minio
from minio.error import S3Error

# create spark session
SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()

# source config
MYSQL_CONFIG = {
    "host": "mysql",
    "port": "3306",
    "database": "pixar_films",
    "tables": ["films", "film_ratings", "genres", "box_office"],
    "user": "lminhnguyet",
    "password": "123"
}

# sink config
MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minio",
    "secret_key": "minio123"
}

BUCKET_NAME = "pixarfilms"
WAREHOUSE_PATH = f"s3a://{BUCKET_NAME}"
LAYER_NAME = "bronze"

DATABASE_NAME = "spark_catalog.default"

SCHEMAS = {
    "brz_films" : """
        number INT NOT NULL,
        film STRING,
        release_date DATE,
        run_time INT,
        film_rating STRING,
        plot STRING,
        sequence_number INT
    """,
    "brz_film_ratings": """
        film STRING NOT NULL,
        rotten_tomatoes_score INT,
        rotten_tomatoes_counts INT,
        metacritic_score INT,
        metacritic_counts INT,
        cinema_score STRING,
        imdb_score DECIMAL(3,1),
        imdb_counts INT,
        sequence_number INT
    """,
    "brz_genres": """
        film STRING NOT NULL,
        category STRING,
        value STRING,
        sequence_number INT
    """, 
    "brz_box_office": """
        film STRING NOT NULL,
        budget INT,
        box_office_us_canada INT,
        box_office_other INT,
        box_office_worldwide INT,
        sequence_number INT
    """
}

# SINK_TABLE = f"{DATABASE_NAME}.{TABLE_NAME}"

def read_from_mysql(table_name="films"):
    df = SPARK.read \
    .format("jdbc") \
    .option("url", f"jdbc:mysql://{MYSQL_CONFIG["host"]}:{MYSQL_CONFIG["port"]}/{MYSQL_CONFIG["database"]}") \
    .option("dbtable", table_name) \
    .option("user", MYSQL_CONFIG["user"]) \
    .option("password", MYSQL_CONFIG["password"]) \
    .load()
    
    df = df.withColumn("sequence_number", f.lit(0))
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

    for table_name in SCHEMAS.keys():
        SPARK.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name} (
                {SCHEMAS[table_name]}
            ) USING iceberg
            LOCATION '{WAREHOUSE_PATH}/{LAYER_NAME}/{table_name[4:]}'
            """)

def main():
    create_bucket()

    for src_table in MYSQL_CONFIG["tables"]:
        print(f"===== FULL LOADING TABLE brz_{src_table} =====")
        df = read_from_mysql(src_table)

        df.printSchema()
        df.show(40)

        # if src_table != "films":
        df.write.format("iceberg").mode("append").saveAsTable(f"{DATABASE_NAME}.brz_{src_table}")

        print(f"===== FULL LOADED TABLE brz_{src_table} =====")
        SPARK.sql(f"SELECT * FROM {DATABASE_NAME}.brz_{src_table}").show(40)

    SPARK.stop()
    
if __name__ == "__main__":
    main()