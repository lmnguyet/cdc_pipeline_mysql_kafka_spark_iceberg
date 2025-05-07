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
    'monthly_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 1 1 * *'
)

optimizing_job = SparkSubmitOperator(
    task_id='optimize_silver_tables',
    application='jobs/optimize.py',
    conn_id='spark-conn',
    verbose=False,
    dag=dag,
    name='optimize_silver_tables',
    application_args=[
        "--tables", "slv_dim_films,slv_dim_genres,slv_fact_film_ratings,slv_fact_film_genres,slv_fact_box_office"
    ]
)

optimizing_job