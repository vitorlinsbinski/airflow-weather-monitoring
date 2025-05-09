from airflow.providers.http.hooks.http import HttpHook
from urllib.parse import quote
from requests import Request

class OpenWeatherHook(HttpHook):
    def __init__(self, latitude=None, longitude=None, city_name=None, state_code=None, country_code=None, conn_id='openweather_api'):
        self.latitude = latitude
        self.longitude = longitude
        self.city_name = city_name
        self.state_code = state_code
        self.country_code = country_code
        super().__init__(http_conn_id=conn_id, method='GET')

    def get_api_key(self):
        conn = self.get_connection(self.http_conn_id)
        return conn.extra_dejson.get('api_key')

    def get_current_weather_data(self):
        endpoint = self.create_weather_url()
        session = self.get_conn()
        url = self.base_url + endpoint
        req = Request(method=self.method, url=url)
        prep = session.prepare_request(req)
        response = self.run_and_check(session, prep, extra_options={})
        return response.json()

    def get_coordinates_by_city(self):
        endpoint = self.create_coordinates_url()
        session = self.get_conn()
        url = self.base_url + endpoint
        req = Request(method=self.method, url=url)
        prep = session.prepare_request(req)
        response = self.run_and_check(session, prep, extra_options={})
        return response.json()

    def get_weather_by_city_name(self, city_name):
        coords = self.get_coordinates_by_city()
        if not coords:
            raise ValueError(f"Nenhuma coordenada encontrada para a cidade '{city_name}'")
        self.latitude = coords[0]['lat']
        self.longitude = coords[0]['lon']
        return self.get_current_weather_data()

    def create_weather_url(self):
        if self.latitude is None or self.longitude is None:
            raise ValueError("Latitude e longitude precisam ser definidas.")
        lang = 'pt_br'
        units = 'metric'
        url = f'/data/2.5/weather?lat={self.latitude}&lon={self.longitude}&units={units}&lang={lang}&appid={self.get_api_key()}'
        return url

    def create_coordinates_url(self):
        if self.city_name is None or self.country_code is None:
            raise ValueError("O nome da cidade e o código do país precisam ser definidos.")
        city_encoded = quote(self.city_name)
        country_encoded = quote(self.country_code)
        state_encoded = quote(self.state_code) if self.state_code else ''
        lang = 'pt_br'
        if state_encoded:
            url_path = f'/geo/1.0/direct?q={city_encoded},{state_encoded},{country_encoded}&limit=1&lang={lang}&appid={self.get_api_key()}'
        else:
            url_path = f'/geo/1.0/direct?q={city_encoded},{country_encoded}&limit=1&lang={lang}&appid={self.get_api_key()}'
        return url_path

    def run(self):
        """
        Este método encapsula o fluxo completo de buscar as coordenadas e depois o tempo.
        """
        if self.city_name:
            weather_data = self.get_weather_by_city_name(self.city_name)
        else:
            weather_data = self.get_current_weather_data()
        print(weather_data)  
        return weather_data
