-- =========================
-- 1) Numeric static features
-- =========================
CREATE TABLE IF NOT EXISTS h3_static_numeric (
    h3_cell VARCHAR(32) PRIMARY KEY,
    altitude_moy DOUBLE PRECISION,
    pente_moy DOUBLE PRECISION,
    dist_route_m DOUBLE PRECISION,
    dist_urbain_m DOUBLE PRECISION,
    ndvi_30d_mean DOUBLE PRECISION
);

-- =========================
-- 2) Descriptive metadata
-- =========================
CREATE TABLE IF NOT EXISTS h3_static_metadata (
    h3_cell VARCHAR(32) PRIMARY KEY,
    wilaya VARCHAR(100),
    daira VARCHAR(100),
    commune VARCHAR(100),
    nom_foret_ou_lieu_dit VARCHAR(255),
    essence_principale VARCHAR(100)
);

-- =========================
-- 3) Final aggregated table
-- =========================
CREATE TABLE IF NOT EXISTS aggregated_all (
    id SERIAL PRIMARY KEY,
    h3_cell VARCHAR(32) NOT NULL,
    event_date DATE NOT NULL,
    window_start BIGINT NOT NULL,
    window_end BIGINT NOT NULL,

    fire_count INT DEFAULT 0,
    fire_declared_count INT DEFAULT 0,
    avg_fire_intensity DOUBLE PRECISION,
    max_fire_intensity DOUBLE PRECISION,
    total_burned_area DOUBLE PRECISION,

    meteo_count INT DEFAULT 0,
    avg_temp DOUBLE PRECISION,
    min_humidity DOUBLE PRECISION,
    max_wind_speed DOUBLE PRECISION,
    avg_vpd DOUBLE PRECISION,
    avg_soil_moisture DOUBLE PRECISION,
    total_precipitation_1d DOUBLE PRECISION,
    total_precipitation_7d DOUBLE PRECISION,

    sensor_count INT DEFAULT 0,
    avg_co2 DOUBLE PRECISION,
    max_co2 DOUBLE PRECISION,
    avg_smoke DOUBLE PRECISION,
    max_smoke DOUBLE PRECISION,
    avg_sensor_temp DOUBLE PRECISION,
    max_sensor_temp DOUBLE PRECISION,
    min_battery DOUBLE PRECISION,

    avg_lat DOUBLE PRECISION,
    avg_lon DOUBLE PRECISION,

    altitude_moy DOUBLE PRECISION,
    pente_moy DOUBLE PRECISION,
    dist_route_m DOUBLE PRECISION,
    dist_urbain_m DOUBLE PRECISION,
    ndvi_30d_mean DOUBLE PRECISION,

    wilaya VARCHAR(100),
    daira VARCHAR(100),
    commune VARCHAR(100),
    nom_foret_ou_lieu_dit VARCHAR(255),
    essence_principale VARCHAR(100),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_aggregated_all_h3_day
ON aggregated_all (h3_cell, event_date);




CREATE TABLE IF NOT EXISTS inference_features (
    id SERIAL PRIMARY KEY,

    source_type VARCHAR(20),
    h3_cell VARCHAR(32),

    event_time BIGINT,
    event_date DATE,

    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,

    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    soil_moisture DOUBLE PRECISION,
    vpd DOUBLE PRECISION,

    co2 DOUBLE PRECISION,
    smoke DOUBLE PRECISION,
    sensor_temperature DOUBLE PRECISION,

    intensity DOUBLE PRECISION,
    burned_area DOUBLE PRECISION
);