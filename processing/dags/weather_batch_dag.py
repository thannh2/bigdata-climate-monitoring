from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_batch_pipeline',
    default_args=default_args,
    description='Chạy luồng Spark Batch tổng hợp dữ liệu Thời tiết & Không khí',
    schedule_interval='0 1 * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['climate', 'batch', 'spark'],
) as dag:

    submit_batch_job = SparkSubmitOperator(
        task_id='run_spark_batch_flow',
        conn_id='spark_default',
        application='/workspace/processing/batch/batch_flow.py',
        name='Airflow_Weather_Batch',
        packages='org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
        conf={
            'spark.driver.memory': '4g',
            'spark.executor.memory': '4g'
        }
    )

    submit_batch_job