from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_submit_demo1',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

submit_job = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='jobs/test.py',
    conn_id='spark-conn',
    verbose=False,
    dag=dag,
    name='airflow-spark-job'
)

submit_job