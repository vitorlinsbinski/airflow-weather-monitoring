from hook.openweather_hook import OpenWeatherHook 

def extract_city_details_from_api(city_name, state_code='MT', country_code='BR'):
    openweather_hook = OpenWeatherHook(city_name=city_name, state_code=state_code, country_code=country_code)
    coordinates_data = openweather_hook.get_coordinates_by_city()

    if not coordinates_data:
        return None

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

    return {
        'city_name': city_name,
        'state_code': state_code,
        'country_code': country_code,
        'latitude': lat,
        'longitude': lon 
    }
