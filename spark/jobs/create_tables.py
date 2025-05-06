from pyspark.sql import SparkSession
import json
import os

# create spark session
SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()

LAYER_NAME = "silver"

def load_params():
    """Load params from params file"""
    global WAREHOUSE_PATH, DATABASE_NAME
    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, 'params.json')

    try:
        with open(params_path) as f:
            params = json.load(f)
        WAREHOUSE_PATH = params["warehouse_path"]
        DATABASE_NAME = params["database"]

    except FileNotFoundError:
        raise

SILVER_SCHEMAS = {
    "slv_dim_films" : """
        id INT NOT NULL,
        film STRING,
        release_date DATE,
        run_time INT,
        film_rating STRING,
        plot STRING
    """,
    "slv_dim_genres": """
        id INT NOT NULL,
        genre STRING
    """,
    "slv_fact_film_ratings": """
        id INT NOT NULL,
        rotten_tomatoes_score INT,
        rotten_tomatoes_counts INT,
        metacritic_score INT,
        metacritic_counts INT,
        cinema_score STRING,
        imdb_score DECIMAL(3,1),
        imdb_counts INT
    """,
    "slv_fact_film_genres": """
        film_id INT NOT NULL,
        genre_id INT NOT NULL,
        is_primary_genre INT
    """,
    "slv_fact_box_office": """
        id INT NOT NULL,
        budget INT,
        box_office_us_canada INT,
        box_office_other INT,
        box_office_worldwide INT
    """
}

CONFIG_SCHEMAS = {
    "cfg_snapshots_tracking": """
        sink_table STRING NOT NULL,
        sink_snapshot_id STRING NOT NULL,
        source_table STRING NOT NULL,
        source_snapshot_id STRING NOT NULL,
        committed_at TIMESTAMP NOT NULL
    """
}

def create_silver_tables():
    for table_name in SILVER_SCHEMAS.keys():
        # SPARK.sql(F"DROP TABLE IF EXISTS {DATABASE_NAME}.{table_name} PURGE")
        SPARK.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name} (
                {SILVER_SCHEMAS[table_name]}
            ) USING iceberg
            LOCATION '{WAREHOUSE_PATH}/{LAYER_NAME}/{table_name}'
            """)

# create snapshots tracking table
# sink_table, sink_snapshot_id, source_table, source_snapshot_id
def create_config_tables():
    for table_name in CONFIG_SCHEMAS.keys():
        # SPARK.sql(F"DROP TABLE IF EXISTS {DATABASE_NAME}.{table_name}")
        SPARK.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name} (
                {CONFIG_SCHEMAS[table_name]}
            ) USING iceberg
            LOCATION '{WAREHOUSE_PATH}/conf/{table_name}'
            """)

def main():
    load_params()
    create_config_tables()
    create_silver_tables()
    SPARK.sql("delete from spark_catalog.default.slv_dim_films")

if __name__ == "__main__":
    main()