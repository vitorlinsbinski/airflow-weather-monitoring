CREATE TABLE IF NOT EXISTS dim_city (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    country_code CHAR(2) NOT NULL,
    state_code CHAR(2),
    latitude DECIMAL(7,4),
    longitude DECIMAL(7,4),
    
    INDEX idx_name (name)  
);
