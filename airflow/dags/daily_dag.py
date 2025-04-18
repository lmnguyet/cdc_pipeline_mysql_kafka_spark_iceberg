from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 0 * * *'
)

bronze_incremental_loading_job = SparkSubmitOperator(
    task_id='bronze_incremental_load',
    application='jobs/bronze_incremental_load.py',
    conn_id='spark-conn',
    verbose=False,
    dag=dag,
    name='bronze_incremental_load',
    conf={
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.spark_catalog.type": "hive",
        "spark.jars": "/opt/airflow/jars/*"
    }
)

bronze_incremental_loading_job