import os
import requests
import gzip
from io import BytesIO
import json

def download_and_extract_city_list():
    url = 'https://bulk.openweathermap.org/sample/city.list.json.gz'
    save_path = './data/raw/cities/city.list.json'

    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    response = requests.get(url)
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
        data = json.load(gz)

    brazil_cities = [city for city in data if city.get('country') == 'BR']

    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(brazil_cities, f, ensure_ascii=False, indent=2)