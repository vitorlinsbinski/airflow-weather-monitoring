import pandas as pd

def upsert_dim_city(df, engine):
    df_city = df[['name', 'coord_lat', 'coord_lon', 'state_code', 'country_code']].drop_duplicates(['coord_lat', 'coord_lon'])
    df_city = df_city.rename(columns={'coord_lat': 'latitude', 'coord_lon': 'longitude'})

    existing = pd.read_sql("SELECT id, name, state_code FROM dim_city", con=engine)
    new = df_city.merge(existing, on=['name', 'state_code'], how='left', indicator=True)
    new = new[new['_merge'] == 'left_only'].drop(columns=['_merge'])

    if not new.empty:
        new.to_sql('dim_city', con=engine, if_exists='append', index=False)

    return pd.read_sql("SELECT id, name, state_code FROM dim_city", con=engine)

def upsert_dim_weather_condition(df, engine):
    df_condition = df[['weather_id', 'main', 'description', 'icon']].drop_duplicates(['weather_id'])
    df_condition = df_condition.rename(columns={'weather_id': 'condition_code', 'main': 'name'})

    existing = pd.read_sql("SELECT condition_code FROM dim_weather_condition", con=engine)
    new = df_condition.merge(existing, on='condition_code', how='left', indicator=True)
    new = new[new['_merge'] == 'left_only'].drop(columns=['_merge'])

    if not new.empty:
        new.to_sql('dim_weather_condition', con=engine, if_exists='append', index=False)

    return pd.read_sql("SELECT id, condition_code FROM dim_weather_condition", con=engine)

def upsert_dim_time(df, engine):
    df_time = df[['datetime_local']].rename(columns={'datetime_local': 'timestamp'}).drop_duplicates()
    df_time['year'] = df_time['timestamp'].dt.year
    df_time['month'] = df_time['timestamp'].dt.month
    df_time['day'] = df_time['timestamp'].dt.day
    df_time['hour'] = df_time['timestamp'].dt.hour
    df_time['minute'] = df_time['timestamp'].dt.minute
    df_time['day_of_week'] = df_time['timestamp'].dt.weekday

    existing = pd.read_sql("SELECT timestamp FROM dim_time", con=engine)
    new = df_time.merge(existing, on='timestamp', how='left', indicator=True)
    new = new[new['_merge'] == 'left_only'].drop(columns=['_merge'])

    if not new.empty:
        new.to_sql('dim_time', con=engine, if_exists='append', index=False)

    return pd.read_sql("SELECT id, timestamp FROM dim_time", con=engine)

def prepare_fact_weather(df):
    df_fact = df[['city_id', 'weather_condition_id', 'time_id', 'main_temp', 'main_feels_like',
                  'main_temp_min', 'main_temp_max', 'main_pressure', 'main_humidity',
                  'wind_speed', 'main_sea_level', 'clouds_all', 'sys_sunrise', 'sys_sunset']].copy()
    
    return df_fact.rename(columns={
        'main_temp': 'main_temperature',
        'main_feels_like': 'temperature_feels_like',
        'main_temp_min': 'min_temperature',
        'main_temp_max': 'max_temperature',
        'main_pressure': 'pressure',
        'main_humidity': 'humidity',
        'main_sea_level': 'sea_level',
        'clouds_all': 'cloud_cover',
        'sys_sunrise': 'sunrise_at',
        'sys_sunset': 'sunset_at'
    })
