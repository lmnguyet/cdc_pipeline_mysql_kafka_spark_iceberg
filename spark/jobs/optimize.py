from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import argparse
import json
import os

SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()

def load_params():
    """Load params from params file"""
    global DATABASE_NAME, ZORDER_COLUMNS
    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, 'params.json')
    try:
        with open(params_path) as f:
            params = json.load(f)
        DATABASE_NAME = params["database"]
        ZORDER_COLUMNS = params["zorder_columns"]
        
        print(f"LOADED PARAMS SUCCESSFULLY.")

    except FileNotFoundError:
        raise ValueError(f"No processing logic found")

def compact(table_fullname, zorder_columns):
    """Compact small files"""
    SPARK.sql(f"""
        CALL spark_catalog.system.rewrite_data_files(
            table => '{table_fullname}',
            strategy => 'sort',
            sort_order => 'zorder({",".join(zorder_columns)})',
            options => map('min-input-files', '3')
        )
    """)

def clean(table_fullname, cleaning_mark):
    """Clean unused snapshots older than 30 days"""
    SPARK.sql(f"""
        CALL spark_catalog.system.expire_snapshots(
            table => '{table_fullname}',
            older_than => TIMESTAMP '{cleaning_mark} 00:00:00',
            retain_last => 3
        )
    """)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tables",
        default="slv_dim_films,slv_dim_genres,slv_fact_film_ratings,slv_fact_film_genres,slv_fact_box_office",
        help="Comma-separated list of tables to process"
    )
    args = parser.parse_args()

    load_params()

    cleaning_mark = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    for table in args.tables.split(','):
        table_fullname = f"{DATABASE_NAME}.{table.strip()}"
        zorder_columns = ZORDER_COLUMNS[table]

        print(f"STARTING COMPACTING FOR TABLE: {table.strip()}")
        compact(table_fullname, zorder_columns)
        print(f"FINISHED COMPACTING FOR TABLE: {table.strip()}")

        print(f"STARTING CLEANING FOR TABLE: {table.strip()}")
        clean(table_fullname, cleaning_mark)
        print(f"FINISHED CLEANING FOR TABLE: {table.strip()}")

    SPARK.stop()

if __name__ == "__main__":
    main()