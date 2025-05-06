# test_spark_job.py
from pyspark.sql import SparkSession
from datetime import datetime
# from pyiceberg.table import Table


def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("IncrementalTestJob") \
        .config("spark.jars", "/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar")\
        .getOrCreate()
    # table_name = "spark_catalog.default.brz_films"
    # key_column = "number"
    try:
        # spark.sql(f"""
        # ALTER TABLE {table_name} SET TBLPROPERTIES (
        # 'write.changelog.enabled' = 'true'
        # );
        # """)
        # current_bronze_snapshot_id = spark.sql(f"""
        #     SELECT snapshot_id 
        #     FROM spark_catalog.default.test.snapshots
        #     ORDER BY committed_at DESC
        #     LIMIT 1
        # """).first()['snapshot_id']

        # spark.conf.set("spark.sql.catalog.spark_catalog.commit.manifest-summary", "true")
        # spark.conf.set("spark.sql.catalog.spark_catalog.commit.manifest-summary.metadata.bronze_snapshot_id", current_bronze_snapshot_id)

        data = [(int(1), 'Alice')]

        df_merge = spark.createDataFrame(data, ['id', 'name'])

        # df_merge.writeTo("spark_catalog.default.test").option("snapshot-property.bronze_snapshot_id", "12").append()
        
        df_merge.createOrReplaceTempView("source_view")

        spark.sql("""
        MERGE INTO spark_catalog.default.test AS target
        USING source_view AS source
        ON target.id = source.id
        WHEN MATCHED THEN
        UPDATE SET name = source.name
        WHEN NOT MATCHED THEN
        INSERT *
        """)

        # spark.sql("""
        # SELECT 
        #     snapshot_id,
        #     summary['bronze_snapshot_id'] AS bronze_version,
        #     summary['commit.manifest-summary.metadata.bronze_snapshot_id'] AS alt_bronze_version
        # FROM spark_catalog.default.test.snapshots
        # ORDER BY committed_at DESC
        # LIMIT 1
        # """).show(truncate=False)

        # spark.sql("DROP VIEW IF EXISTS my_changelog_view1")
        # spark.sql(f"""
        # CALL spark_catalog.system.create_changelog_view(
        # table => 'spark_catalog.default.brz_films',
        # options => map('start-snapshot-id','1805050604627168782','end-snapshot-id', '6150828307555235674'),
        # changelog_view => 'my_changelog_view1',
        # identifier_columns => array('number')
        # )
        # """)
        # spark.sql("select * from my_changelog_view1 limit 40").show(40)



        # df = spark.sql(f"""
        # WITH CTE AS (
        #     SELECT *, ROW_NUMBER() OVER(PARTITION BY number ORDER BY _change_ordinal DESC) AS RN
        #     FROM my_changelog_view
        #     WHERE _change_type <> 'UPDATE_BEFORE'
        #     limit 10
        # )
        # SELECT *
        # FROM CTE
        # WHERE RN = 1
        # """)
        # df.printSchema()
        # df.show()

        # from pyiceberg.catalog import load_catalog

        # catalog = load_catalog(
        #     "spark_catalog",
        #     **{
        #         "uri": "thrift://hive-metastore:9083",
        #         "warehouse": "s3a://pixarfilms/",
        #         "type": "hive",
        #         "s3.region": "us-east-1",
        #         "io-impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        #         "s3.endpoint": "http://minio:9000",
        #         "s3.access-key-id": "minio",
        #         "s3.secret-access-key": "minio123"
        #     }
        # )

        # table = catalog.load_table("default.test")
        # current_snapshot = table.current_snapshot()

        # print(current_snapshot.snapshot_id)

        # # table._append_snapshot_producer({"bronze_snapshot_id": "24"})

        # with table.transaction() as tx:
        #     # updated_snapshot = current_snapshot.with_properties({"bronze_snapshot_id": "24"})
        #     # tx.set_current_snapshot(updated_snapshot.snapshot_id)
        #     # tx.update_snapshot().set_properties({"bronze_snapshot_id": "24"}).commit()
        #     tx._append_snapshot_producer({"bronze_snapshot_id": "24"})

        return 0
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return 1
    finally:
        spark.stop()

if __name__ == "__main__":
    main()