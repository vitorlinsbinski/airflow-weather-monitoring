CREATE TABLE IF NOT EXISTS dim_city (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    country_code CHAR(2) NOT NULL,
    state_code CHAR(2),
    latitude DECIMAL(7,4),
    longitude DECIMAL(7,4),
    
    INDEX idx_name (name)  
);

CREATE TABLE IF NOT EXISTS dim_weather_condition (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    condition_code INT NOT NULL,
    name VARCHAR(50) NOT NULL,
    description VARCHAR(120) NOT NULL,
    icon VARCHAR(50),

    UNIQUE(condition_code)
);

CREATE TABLE IF NOT EXISTS dim_time (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    timestamp DATETIME NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL,
    day_of_week INT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_weather (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    city_id INT NOT NULL,
    weather_condition_id INT NOT NULL,
    time_id INT NOT NULL,
    main_temperature DECIMAL(5,2) NOT NULL,
    temperature_feels_like DECIMAL(5,2) NOT NULL,
    min_temperature DECIMAL(5,2) NOT NULL,
    max_temperature DECIMAL(5,2) NOT NULL,
    pressure INT NOT NULL,
    humidity INT NOT NULL,
    wind_speed DECIMAL(5,2) NOT NULL,
    sea_level INT,
    cloud_cover INT,
    sunrise_at TIME,
    sunset_at TIME,
    
    FOREIGN KEY (city_id) REFERENCES dim_city(id),
    FOREIGN KEY (weather_condition_id) REFERENCES dim_weather_condition(id),
    FOREIGN KEY (time_id) REFERENCES dim_time(id)
);