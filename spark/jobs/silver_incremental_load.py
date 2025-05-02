from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from minio import Minio
from minio.error import S3Error

# create spark session
SPARK = SparkSession.builder \
        .appName("SpakApp") \
        .getOrCreate()
        
BUCKET_NAME = "pixarfilms"
WAREHOUSE_PATH = f"s3a://{BUCKET_NAME}"
LAYER_NAME = "silver"

DATABASE_NAME = "spark_catalog.default"

def load_incremental_raw_data(bronze_table, silver_table, key_columns):
    """Return incremental raw data from the bronze layer."""
    # get current bronze snapshot id
    current_bronze_snapshot_id = spark.sql(f"""
        SELECT snapshot_id 
        FROM {bronze_table}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """).first()["snapshot_id"]

    # get latest bronze snapshot id that has been loaded to silver
    latest_bronze_snapshot_id_on_silver = spark.sql(f"""
        
    """).first()["snapshot_id"] # get latest bronze snapshot id on silver table

    # create changelog from 2 snapshot id of bronze table
    key_str = ",".join(["'" + key + "'" for key in key_columns])

    spark.sql("DROP VIEW IF EXISTS tmp_changelog")
    spark.sql(f"""
        CALL spark_catalog.system.create_changelog_view(
        table => '{bronze_table}',
        options => map('start-snapshot-id','{latest_bronze_snapshot_id_on_silver}','end-snapshot-id', '{current_bronze_snapshot_id}'),
        changelog_view => 'tmp_changelog',
        identifier_columns => array({key_str})
    )
    """)

    # processing merging data
    df = spark.sql(f"""
    WITH CTE AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY number ORDER BY _change_ordinal DESC) AS rn
        FROM tmp_changelog
        WHERE _change_type <> 'UPDATE_BEFORE'
    )
    SELECT *
    FROM CTE
    WHERE rn = 1
    """)
    return df

# process raw data
def process_raw_data():
    pass

# merge processed data
def merge_process_data():
    """Atomic loading incremental data from bronze to silver layer."""
    # merge data to silver table

    # append snapshots tracking table
    pass

def main():
    raw_df = load_incremental_raw_data()

    processed_df = process_raw_data()

    merge_process_data()

    SPARK.stop()
    
if __name__ == "__main__":
    main()