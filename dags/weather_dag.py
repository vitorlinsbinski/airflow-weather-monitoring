from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.tasks.weather_tasks import (
    create_timestamp_and_directories,
    get_weather_data,
    save_weather_data,
    read_raw_weather_data,
    normalize_weather_data,
    cast_and_enrich_weather_data,
    save_transformed_weather_data,
    prepare_for_dw,
    load_to_dw
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'vitorlinsbinski',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_dag',
    description='ETL de clima com funções modulares',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

prepare_database = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='mysql_conn',
    sql='./scripts/db/sql/create_tables.sql',
    dag=dag
)

create_directories = PythonOperator(
    task_id='create_timestamp_and_directories',
    python_callable=create_timestamp_and_directories,
    provide_context=True,
    dag=dag
)

fetch_data_from_api = PythonOperator(
    task_id='get_weather_data_from_api',
    python_callable=get_weather_data,
    provide_context=True,
    dag=dag
)

save_raw_data = PythonOperator(
    task_id='save_raw_weather_data',
    python_callable=save_weather_data,
    provide_context=True,
    dag=dag
)

read_raw_data = PythonOperator(
    task_id='read_raw_weather_data',
    python_callable=read_raw_weather_data,
    provide_context=True,
    dag=dag
)

normalize_data = PythonOperator(
    task_id='normalize_weather_data',
    python_callable=normalize_weather_data,
    provide_context=True,
    dag=dag
)

transform_raw_data = PythonOperator(
    task_id='cast_and_enrich_weather_data',
    python_callable=cast_and_enrich_weather_data,
    provide_context=True,
    dag=dag
)

save_transformed_data = PythonOperator(
    task_id='save_transformed_weather_data',
    python_callable=save_transformed_weather_data,
    provide_context=True,
    dag=dag
)

prepare_data_for_dw = PythonOperator(
    task_id='prepare_for_dw',
    python_callable=prepare_for_dw,
    provide_context=True,
    dag=dag
)

load_data_to_dw = PythonOperator(
    task_id='load_to_dw',
    python_callable=load_to_dw,
    provide_context=True,
    dag=dag
)

prepare_database >> fetch_data_from_api
create_directories >> fetch_data_from_api
fetch_data_from_api >> save_raw_data >> read_raw_data >> normalize_data >> transform_raw_data >> save_transformed_data >> prepare_data_for_dw >> load_data_to_dw

