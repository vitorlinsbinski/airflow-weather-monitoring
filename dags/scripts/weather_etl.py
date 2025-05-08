import os
import json
import pytz

from hook.openweather_hook import OpenWeatherHook
from scripts.db_weather_operations import find_city_coordinates, create_new_city


def extract_coordinates(city_name, state_code='MT', country_code='BR'):
    lat, lon = find_city_coordinates(city_name)

    if lat is not None and lon is not None:
        print('Getting coordinates from database')
        return lat, lon
    else:
        print('Getting coordinates from API')
        openweather_hook = OpenWeatherHook(city_name=city_name, state_code=state_code, country_code=country_code)
        coordinates_data = openweather_hook.get_coordinates_by_city()

        if not coordinates_data:
            return None, None

        lat = coordinates_data[0].get('lat')
        lon = coordinates_data[0].get('lon')
        country_code = coordinates_data[0].get('country')

        state_mapping = {
            'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM', 'Bahia': 'BA',
            'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES', 'Goiás': 'GO',
            'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG',
            'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE', 'Piauí': 'PI',
            'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN', 'Rio Grande do Sul': 'RS',
            'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC', 'São Paulo': 'SP',
            'Sergipe': 'SE', 'Tocantins': 'TO'
        }

        state = coordinates_data[0].get('state')
        state_code = state_mapping.get(state)

        create_new_city(city_name=city_name, latitude=lat, longitude=lon, country_code=country_code, state_code=state_code)

    return lat, lon


def create_timestamp_and_directories(**context):
    execution_date_utc = context['data_interval_end']
    cuiaba_tz = pytz.timezone('America/Cuiaba')
    execution_date_local = execution_date_utc.astimezone(cuiaba_tz)
    timestamp = execution_date_local.strftime('%Y%m%d_%H%M%S')

    base_path = './data/raw'
    full_path = os.path.join(base_path, timestamp)
    os.makedirs(base_path, exist_ok=True)
    os.makedirs(full_path, exist_ok=True)

    context['ti'].xcom_push(key='timestamp', value=timestamp)
    context['ti'].xcom_push(key='full_path', value=full_path)


def load_cities(**context):
    file_path = './data/raw/region/cities.json'
    with open(file_path, 'r') as file:
        cities = json.load(file)
    context['ti'].xcom_push(key='cities', value=cities)


def get_weather_data(**context):
    ti = context['ti']
    cities = ti.xcom_pull(key='cities', task_ids='load_cities')
    weather_data_cities = []

    for city in cities:
        city_name = city.get('name')
        state_code = city.get('state_code')
        country_code = city.get('country_code')

        lat, lon = extract_coordinates(city_name, state_code, country_code)

        if lat is None or lon is None:
            continue

        openweather_hook = OpenWeatherHook(lat, lon)
        weather_data = openweather_hook.get_current_weather_data()

        if weather_data:
            weather_data_cities.append(weather_data)

    ti.xcom_push(key='weather_data', value=weather_data_cities)


def save_weather_data(**context):
    ti = context['ti']
    weather_data = ti.xcom_pull(key='weather_data', task_ids='get_weather_data')
    full_path = ti.xcom_pull(key='full_path', task_ids='create_timestamp_and_directories')
    timestamp = ti.xcom_pull(key='timestamp', task_ids='create_timestamp_and_directories')

    filename = os.path.join(full_path, f'weather_{timestamp}.json')
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=4)

    print(f'Dados salvos em {filename}')
