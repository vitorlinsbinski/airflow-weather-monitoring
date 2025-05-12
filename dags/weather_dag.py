from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.weather_etl import (
    create_timestamp_and_directories,
    read_cities_from_local,
    get_weather_data,
    save_weather_data,
    read_raw_weather_data,
    normalize_weather_data,
    cast_and_enrich_weather_data,
    save_transformed_weather_data
)

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

# Tarefa 1: Criar timestamp e diretórios
t1 = PythonOperator(
    task_id='create_timestamp_and_directories',
    python_callable=create_timestamp_and_directories,
    provide_context=True,
    dag=dag
)

# Tarefa 2: Carregar as cidades
t2 = PythonOperator(
    task_id='read_cities_from_local',
    python_callable=read_cities_from_local,
    provide_context=True,
    dag=dag
)

# Tarefa 3: Obter dados climáticos
t3 = PythonOperator(
    task_id='get_weather_data_from_api',
    python_callable=get_weather_data,
    provide_context=True,
    dag=dag
)

# Tarefa 4: Salvar dados climáticos
t4 = PythonOperator(
    task_id='save_raw_weather_data',
    python_callable=save_weather_data,
    provide_context=True,
    dag=dag
)

t5_1 = PythonOperator(
    task_id='read_raw_weather_data',
    python_callable=read_raw_weather_data,
    provide_context=True,
    dag=dag
)

t5_2 = PythonOperator(
    task_id='normalize_weather_data',
    python_callable=normalize_weather_data,
    provide_context=True,
    dag=dag
)

t5_3 = PythonOperator(
    task_id='cast_and_enrich_weather_data',
    python_callable=cast_and_enrich_weather_data,
    provide_context=True,
    dag=dag
)

t5_4 = PythonOperator(
    task_id='save_transformed_weather_data',
    python_callable=save_transformed_weather_data,
    provide_context=True,
    dag=dag
)

# Definindo a ordem das tarefas
t1 >> t2 >> t3 >> t4 >> t5_1 >> t5_2 >> t5_3 >> t5_4

