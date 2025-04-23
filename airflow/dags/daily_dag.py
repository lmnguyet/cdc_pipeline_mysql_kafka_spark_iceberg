from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1, 23, 0, tzinfo=pendulum.timezone("Asia/Ho_Chi_Minh")),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 23 * * *'
)

bronze_incremental_loading_job = SparkSubmitOperator(
    task_id='bronze_incremental_load',
    application='jobs/bronze_incremental_load.py',
    conn_id='spark-conn',
    verbose=False,
    dag=dag,
    name='bronze_incremental_load'
)

silver_loading_job = SparkSubmitOperator(
    task_id='silver_load',
    application='jobs/silver_load.py',
    conn_id='spark-conn',
    verbose=False,
    dag=dag,
    name='silver_load'
)

bronze_incremental_loading_job >> silver_loading_job