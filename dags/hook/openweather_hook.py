from airflow.providers.http.hooks.http import HttpHook
from urllib.parse import quote

class OpenWeatherHook(HttpHook):
    def __init__(self, latitude=None, longitude=None, city_name=None, state_code=None, country_code=None, conn_id='openweather_api'):
        self.latitude = latitude
        self.longitude = longitude
        self.city_name = city_name
        self.state_code = state_code
        self.country_code = country_code

        super().__init__(http_conn_id=conn_id, method='GET')

    def get_api_key(self):
        """
        Recupera a chave da API da conexão configurada no Airflow.
        """
        conn = self.get_connection(self.http_conn_id)
        return conn.extra_dejson.get('api_key')

    def get_current_weather_data(self):
        """
        Consulta a previsão do tempo usando latitude e longitude.
        """
        endpoint = self.create_weather_url()
        print("ENDPOINT: ", endpoint)
        response = self.run(endpoint)
        return response.json()

    def get_coordinates_by_city(self):
        """
        Consulta as coordenadas de uma cidade usando o endpoint de geocodificação.
        """
        endpoint = self.create_coordinates_url()
        print("ENDPOINT:", endpoint)
        response = self.run(endpoint)  # agora o endpoint é só o path
        return response.json()

    
    def get_weather_by_city_name(self, city_name):
        """
        Consulta o tempo atual baseado no nome da cidade (fazendo duas chamadas: coordenadas e previsão).
        """
        coords = self.get_coordinates_by_city(city_name)
        if not coords:
            raise ValueError(f"Nenhuma coordenada encontrada para a cidade '{city_name}'")
        
        self.latitude = coords[0]['lat']
        self.longitude = coords[0]['lon']
        return self.get_current_weather_data()

    def create_weather_url(self):
        """
        Monta a URL do endpoint de previsão do tempo com base nas coordenadas.
        """
        if self.latitude is None or self.longitude is None:
            raise ValueError("Latitude e longitude precisam ser definidas.")
        
        lang = 'pt_br'
        units = 'metric'
        url = f'/data/2.5/weather?lat={self.latitude}&lon={self.longitude}&units={units}&lang={lang}&appid={self.get_api_key()}'
        return url
    
    def create_coordinates_url(self):
        """
        Monta o path do endpoint de geocodificação (relativo ao host).
        """
        if self.city_name is None or self.country_code is None:
            raise ValueError("O nome da cidade e o código do país precisam ser definidos.")
        
        city_encoded = quote(self.city_name)
        country_encoded = quote(self.country_code)
        state_encoded = quote(self.state_code) if self.state_code else ''  # Verifica se state_code existe

        lang = 'pt_br'
        
        if state_encoded:
            url_path = f'/geo/1.0/direct?q={city_encoded},{state_encoded},{country_encoded}&limit=1&lang={lang}&appid={self.get_api_key()}'
        else:
            url_path = f'/geo/1.0/direct?q={city_encoded},{country_encoded}&limit=1&lang={lang}&appid={self.get_api_key()}'
        
        return url_path



