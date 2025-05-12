import os
import json

from hook.openweather_hook import OpenWeatherHook
from scripts.db.db_operations import get_engine
from airflow.providers.mysql.hooks.mysql import MySqlHook

import pandas as pd
import numpy as np

from scripts.utils.date_operations import format_date_to_utc
from scripts.api.weather_api import extract_city_details_from_api

from scripts.utils.weather_load_dimensions import prepare_fact_weather, upsert_dim_weather_condition,upsert_dim_time,upsert_dim_city

def create_timestamp_and_directories(**context):
    execution_date_utc = context['data_interval_end']
    timestamp = format_date_to_utc(execution_date_utc, 'America/Cuiaba')

    base_path = './data/raw'
    full_path = os.path.join(base_path, timestamp)
    os.makedirs(base_path, exist_ok=True)
    os.makedirs(full_path, exist_ok=True)

    context['ti'].xcom_push(key='timestamp', value=timestamp)
    context['ti'].xcom_push(key='full_path', value=full_path)


def read_cities_from_local(**context):
    file_path = './data/raw/region/cities.json'
    with open(file_path, 'r') as file:
        cities = json.load(file)
    context['ti'].xcom_push(key='cities', value=cities)


def get_weather_data(**context):
    ti = context['ti']
    cities = ti.xcom_pull(key='cities', task_ids='read_cities_from_local')
    weather_data_cities = []

    for city in cities:
        city_name = city.get('name')
        state_code = city.get('state_code')
        country_code = city.get('country_code')

        weather_data = []

        city_details = extract_city_details_from_api(city_name=city_name, state_code=state_code, country_code=country_code)

        if not city_details:
            continue

        city_name = city_details.get('city_name')
        state_code = city_details.get('state_code')
        country_code = city_details.get('country_code')
        latitude = city_details.get('latitude')
        longitude = city_details.get('longitude')

        openweather_hook = OpenWeatherHook(latitude=latitude, longitude=longitude)
        weather_data = openweather_hook.run()

        if weather_data:
            weather_data.update({
                'name': city_name,
                'state_code': state_code,
                'country_code': country_code,
            })
            weather_data_cities.append(weather_data)
        else:
            continue

    ti.xcom_push(key='weather_data', value=weather_data_cities)


def save_weather_data(**context):
    ti = context['ti']
    weather_data = ti.xcom_pull(key='weather_data', task_ids='get_weather_data_from_api')
    full_path = ti.xcom_pull(key='full_path', task_ids='create_timestamp_and_directories')
    timestamp = ti.xcom_pull(key='timestamp', task_ids='create_timestamp_and_directories')

    filename = os.path.join(full_path, f'weather_{timestamp}.json')
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=4)

    print(f'Dados salvos em {filename}')
    
def read_raw_weather_data(**context):
    execution_date_utc = context['data_interval_end']
    timestamp = format_date_to_utc(execution_date_utc, 'America/Cuiaba')

    base_path = './data/raw'
    file_name = f'weather_{timestamp}.json'
    target_path = os.path.join(base_path, timestamp, file_name)

    with open(target_path, 'r', encoding='utf-8') as file:
        weather_data = json.load(file)

    context['ti'].xcom_push(key='weather_data', value=weather_data)
    context['ti'].xcom_push(key='timestamp', value=timestamp)

def normalize_weather_data(**context):
    weather_data = context['ti'].xcom_pull(key='weather_data', task_ids='read_raw_weather_data')

    weather_df = pd.json_normalize(
        weather_data,
        record_path=['weather'],
        meta=[
            'base', 'visibility', 'dt', 'timezone', 'name', 'cod', 'state_code', 'country_code',
            ['coord', 'lon'], ['coord', 'lat'],
            ['main', 'temp'], ['main', 'feels_like'], ['main', 'temp_min'], ['main', 'temp_max'],
            ['main', 'pressure'], ['main', 'humidity'], ['main', 'sea_level'],
            ['wind', 'speed'],
            ['clouds', 'all'],
            ['sys', 'sunrise'], ['sys', 'sunset']
        ],
        sep='_'
    )

    weather_df.drop(columns=['base', 'visibility'], inplace=True)

    context['ti'].xcom_push(key='weather_df', value=weather_df.to_json())

def cast_and_enrich_weather_data(**context):
    weather_df = pd.read_json(context['ti'].xcom_pull(key='weather_df', task_ids='normalize_weather_data'))

    weather_df = weather_df.astype({
        'id': int,
        'timezone': int,
        'cod': int,
        'coord_lon': np.float64,
        'coord_lat': np.float64,
        'main_temp': np.float64,
        'main_temp_min': np.float64,
        'main_temp_max': np.float64,
        'main_pressure': int,
        'main_humidity': int,
        'main_sea_level': int,
        'wind_speed': np.float32,
        'clouds_all': int,
        'sys_sunrise': int,
        'sys_sunset': int
    })

    weather_df = weather_df.rename(columns={
        'id': 'weather_id'
    })

    weather_df['datetime_utc'] = pd.to_datetime(weather_df['dt'], unit='s', utc=True)
    weather_df['datetime_local'] = weather_df['datetime_utc'] + pd.to_timedelta(weather_df['timezone'], unit='s')
    weather_df['datetime_local'] = weather_df['datetime_local'].dt.tz_localize(None)

    weather_df['sys_sunrise'] = pd.to_datetime(weather_df['sys_sunrise'], unit='s') + pd.to_timedelta(weather_df['timezone'], unit='s')
    weather_df['sys_sunrise'] = weather_df['sys_sunrise'].dt.strftime('%H:%M:%S')
    weather_df['sys_sunrise'] = pd.to_datetime(weather_df['sys_sunrise'], format='%H:%M:%S').dt.time

    weather_df['sys_sunset'] = pd.to_datetime(weather_df['sys_sunset'], unit='s') + pd.to_timedelta(weather_df['timezone'], unit='s')
    weather_df['sys_sunset'] = weather_df['sys_sunset'].dt.strftime('%H:%M:%S')
    weather_df['sys_sunset'] = pd.to_datetime(weather_df['sys_sunset'], format='%H:%M:%S').dt.time

    weather_df['description'] = weather_df['description'].str.capitalize()

    weather_df.drop(columns=['dt', 'timezone', 'datetime_utc'], inplace=True)

    # Salva Parquet em um local temporário
    base_path = './data/temp'
    os.makedirs(base_path, exist_ok=True)
    parquet_path = os.path.join(base_path, 'weather_df_enriched.parquet')
    weather_df.to_parquet(parquet_path)

    print(weather_df.info())

    context['ti'].xcom_push(key='weather_df_parquet_path', value=parquet_path)

def save_transformed_weather_data(**context):
    timestamp = context['ti'].xcom_pull(key='timestamp', task_ids='read_raw_weather_data')
    parquet_path = context['ti'].xcom_pull(key='weather_df_parquet_path', task_ids='cast_and_enrich_weather_data')

    weather_df = pd.read_parquet(parquet_path)

    print(weather_df.info())

    base_path_transformed = './data/transformed'
    full_path = os.path.join(base_path_transformed, timestamp)
    os.makedirs(full_path, exist_ok=True)

    file_name = f'weather_transformed_{timestamp}.parquet'
    outfile_path = os.path.join(full_path, file_name)

    weather_df.to_parquet(outfile_path, index=False)

def prepare_for_dw(**context):
    timestamp = context['ti'].xcom_pull(key='timestamp', task_ids='read_raw_weather_data')
    base_path = './data/transformed'
    file_name = f'weather_transformed_{timestamp}.parquet'
    target_path = os.path.join(base_path, timestamp, file_name)
    df = pd.read_parquet(target_path)

    engine = get_engine()

    df_city_ids = upsert_dim_city(df, engine)
    df = df.merge(df_city_ids, how='left', right_on=['latitude', 'longitude'], left_on=['coord_lat', 'coord_lon'])
    df = df.rename(columns={'id': 'city_id'})

    df_condition_ids = upsert_dim_weather_condition(df, engine)
    df = df.merge(df_condition_ids, how='left', left_on='weather_id', right_on='condition_code')
    df = df.rename(columns={'id': 'weather_condition_id'})

    df_time_ids = upsert_dim_time(df, engine)
    df = df.merge(df_time_ids, how='left', left_on='datetime_local', right_on='timestamp')
    df = df.rename(columns={'id': 'time_id'})

    df_fact_weather = prepare_fact_weather(df)

    base_path_curated = './data/curated'
    full_path = os.path.join(base_path_curated, timestamp)
    os.makedirs(full_path, exist_ok=True)

    file_name = f'weather_curated_{timestamp}.parquet'
    outfile_path = os.path.join(full_path, file_name)

    df_fact_weather.to_parquet(outfile_path, index=False)

def load_to_dw(**context):
    timestamp = context['ti'].xcom_pull(key='timestamp', task_ids='read_raw_weather_data')
    base_path_curated = './data/curated'
    file_name = f'weather_curated_{timestamp}.parquet'
    infile_path = os.path.join(base_path_curated, timestamp, file_name)

    df = pd.read_parquet(infile_path)
    engine = get_engine()
    df.to_sql('fact_weather', con=engine, if_exists='append', index=False)



