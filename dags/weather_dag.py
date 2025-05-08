from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.weather_etl import (
    create_timestamp_and_directories,
    load_cities,
    get_weather_data,
    save_weather_data
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
    task_id='load_cities',
    python_callable=load_cities,
    provide_context=True,
    dag=dag
)

# Tarefa 3: Obter dados climáticos
t3 = PythonOperator(
    task_id='get_weather_data',
    python_callable=get_weather_data,
    provide_context=True,
    dag=dag
)

# Tarefa 4: Salvar dados climáticos
t4 = PythonOperator(
    task_id='save_weather_data',
    python_callable=save_weather_data,
    provide_context=True,
    dag=dag
)

# Definindo a ordem das tarefas
t1 >> t2 >> t3 >> t4
