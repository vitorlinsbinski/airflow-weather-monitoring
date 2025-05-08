from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.weather_etl import extract_data

default_args = {
    'owner': 'vitorlinsbinski',
    'start_date': datetime.now() - timedelta(days=1),
    'retires': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_dag',
    description='Capture current weather data from API',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False
)

extract_task = PythonOperator(
    task_id = 'extract_weather_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

extract_task