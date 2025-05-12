from airflow.providers.mysql.hooks.mysql import MySqlHook

CITY_TABLE_NAME = 'dim_city'

def fetch_cities():
    hook = MySqlHook(mysql_conn_id='mysql_conn')
    sql = "SELECT name, state_code, country_code, latitude, longitude FROM dim_city"
    result = hook.get_records(sql)
    
    cities = []
    for row in result:
        city = {
            'name': row[0],
            'state_code': row[1],
            'country_code': row[2],
            'latitude': row[3],
            'longitude': row[4]
        }
        cities.append(city)
    return cities

def find_city_coordinates(city_name):
    """
    Busca latitude e longitude de uma cidade pelo nome.
    Retorna (latitude, longitude) se encontrado, senão (None, None).
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    sql = f'''
        SELECT latitude, longitude
        FROM {CITY_TABLE_NAME}
        WHERE name = %s
    '''
    result = mysql_hook.get_records(sql, parameters=(city_name,))

    if result:
        return result[0]  
    else:
        return None, None


def create_new_city(city_name, latitude, longitude, state_code=None, country_code='BR'):
    """
    Insere uma nova cidade na tabela `dim_city`.
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    sql = f'''
        INSERT INTO {CITY_TABLE_NAME} (name, state_code, country_code, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s)
    '''

    mysql_hook.run(sql, parameters=(city_name, state_code, country_code, latitude, longitude))
    print(f'City "{city_name}" inserted successfully.')
