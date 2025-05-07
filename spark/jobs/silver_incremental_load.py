from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from minio import Minio
from minio.error import S3Error
import argparse
import json
import importlib
import os

# create spark session
SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()
        
LAYER_NAME = "silver"

TABLE_KEYS = {}
SOURCE_TABLES = {}

def load_params():
    """Load params from params file"""
    global WAREHOUSE_PATH, DATABASE_NAME, TABLE_KEYS, SOURCE_TABLES
    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, 'params.json')
    try:
        with open(params_path) as f:
            params = json.load(f)
        WAREHOUSE_PATH = params["warehouse_path"]
        DATABASE_NAME = params["database"]
        TABLE_KEYS = params["table_keys"]
        SOURCE_TABLES = params["source_tables"]
        
        print(f"LOADED PARAMS SUCCESSFULLY.")

    except FileNotFoundError:
        raise ValueError(f"No processing logic found")

def get_current_snapshot_id(table_fullname):
    """Get current snapshot id of a table."""
    current_snapshot_id = SPARK.sql(f"""
        SELECT snapshot_id 
        FROM {table_fullname}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """)
    return current_snapshot_id.first()[0]

def get_latest_source_snapshot_id(sink_table_fullname, source_table_fullname):
    """Get latest source snapshot id of a table."""
    latest_source_snapshot_id = SPARK.sql(f"""
        SELECT source_snapshot_id
        FROM {DATABASE_NAME}.cfg_snapshots_tracking
        WHERE sink_table = '{sink_table_fullname}' AND source_table = '{source_table_fullname}'
        ORDER BY committed_at DESC
        LIMIT 1
    """)
    if latest_source_snapshot_id.count() == 0:
        return '0'
    return latest_source_snapshot_id.first()[0]

def write_snapshot_tracking(sink_table_fullname, source_table_fullname, source_snapshot_id):
    """Write new snapshot tracking into snapshots tracking table."""
    current_sink_snapshot_id = get_current_snapshot_id(sink_table_fullname)

    committed_time = SPARK.sql(f"""
        SELECT committed_at 
        FROM {sink_table_fullname}.snapshots
        WHERE snapshot_id = {current_sink_snapshot_id}
    """).first()[0]

    SPARK.sql(f"""
        INSERT INTO {DATABASE_NAME}.cfg_snapshots_tracking (sink_table, sink_snapshot_id, source_table, source_snapshot_id, committed_at)
        VALUES ('{sink_table_fullname}', '{current_sink_snapshot_id}', '{source_table_fullname}', '{source_snapshot_id}', TIMESTAMP '{committed_time}')
    """)

def load_incremental_raw_data(table_fullname, start_snapshot_id, end_snapshot_id, key_columns):
    """Return incremental raw data from a table."""
    key_str = ",".join(["'" + key + "'" for key in key_columns])

    if start_snapshot_id == '0':
        map_str = f"'end-snapshot-id', '{end_snapshot_id}'"
    else:
        map_str = f"'start-snapshot-id','{start_snapshot_id}','end-snapshot-id', '{end_snapshot_id}'"

    SPARK.sql("DROP VIEW IF EXISTS tmp_changelog")
    SPARK.sql(f"""
        CALL spark_catalog.system.create_changelog_view(
        table => '{table_fullname}',
        options => map({map_str}),
        changelog_view => 'tmp_changelog',
        identifier_columns => array({key_str})
    )
    """)

    df = SPARK.sql(f"""
    WITH CTE AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY {','.join(key_columns)} ORDER BY _change_ordinal DESC) AS rn
        FROM tmp_changelog
        WHERE _change_type <> 'UPDATE_BEFORE'
    )
    SELECT *
    FROM CTE
    WHERE rn = 1
    """)

    columns_to_keep = [col for col in df.columns if (not col.startswith('_') or col =="_change_type") and col != "rn"]

    df = df.select(*columns_to_keep)
    return df

# process raw data
def process_raw_data(raw_df, table_name):
    try:
        module = importlib.import_module(f'silver.process_scripts')
        
        process_func = getattr(module, f'process_{table_name}')
        
        processed_df = process_func(SPARK, raw_df)
        
        print(f"SUCCESSFULLY PROCESS TABLE: {table_name}")
        return processed_df
    except Exception as e:
        print(f"FAILED TO PROCESS TABLE {table_name}: {str(e)}")
        raise

# merge processed data
def merge_processed_data(df, table_fullname, key_columns):
    tmp_view = f"tmp_{table_fullname.split(".")[-1]}"
    df.createOrReplaceTempView(tmp_view)

    join_str = " AND ".join([f"target.{key} = source.{key}" for key in key_columns])

    SPARK.sql(f"""
        MERGE INTO {table_fullname} target
        USING {tmp_view} source
        ON {join_str}
        WHEN MATCHED AND source._change_type = 1 THEN UPDATE SET *
        WHEN MATCHED AND source._change_type = 2 THEN DELETE
        WHEN NOT MATCHED THEN INSERT *
    """)

def load_to_silver(table):
    """Atomic loading incremental data from bronze to silver layer."""
    bronze_table = SOURCE_TABLES[table][0]

    silver_table_fullname = f"{DATABASE_NAME}.{table}"
    bronze_table_fullname = f"{DATABASE_NAME}.{bronze_table}"

    # get current bronze snapshot id
    current_bronze_snapshot_id = get_current_snapshot_id(bronze_table_fullname)

    # get latest bronze snapshot id that has been loaded to silver table
    latest_bronze_snapshot_id = get_latest_source_snapshot_id(silver_table_fullname, bronze_table_fullname)

    if str(current_bronze_snapshot_id) == str(latest_bronze_snapshot_id):
        print(f"ALREADY LOADED NEWEST DATA TO TABLE {table}")
        return

    # load incremental raw data
    raw_df = load_incremental_raw_data(bronze_table_fullname, latest_bronze_snapshot_id, current_bronze_snapshot_id, TABLE_KEYS[bronze_table])

    # process raw data
    processed_df = process_raw_data(raw_df, table)

    # merge data to silver table
    merge_processed_data(processed_df, silver_table_fullname, TABLE_KEYS[table])

    # append snapshots tracking table
    write_snapshot_tracking(silver_table_fullname, bronze_table_fullname, current_bronze_snapshot_id)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tables",
        default="slv_dim_films,slv_dim_genres,slv_fact_film_ratings,slv_fact_film_genres,slv_fact_box_office",
        help="Comma-separated list of tables to process"
    )
    args = parser.parse_args()

    load_params()

    for table in args.tables.split(','):
        print(f"STARTING PROCESSING FOR TABLE: {table.strip()}")
        load_to_silver(table.strip())
        print(f"FINISHED PROCESSING FOR TABLE: {table.strip()}")

    SPARK.stop()
    
if __name__ == "__main__":
    main()