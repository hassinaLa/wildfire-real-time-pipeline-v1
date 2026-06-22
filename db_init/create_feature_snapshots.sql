CREATE TABLE IF NOT EXISTS feature_snapshots (
    id BIGSERIAL PRIMARY KEY,
    h3_cell VARCHAR(32) NOT NULL,
    h3_commune VARCHAR(32) NOT NULL,
    event_time BIGINT NOT NULL,
    event_date DATE NOT NULL,

    h3_center_lat DOUBLE PRECISION,
    h3_center_lon DOUBLE PRECISION,
    altitude_moy DOUBLE PRECISION,
    pente_moy DOUBLE PRECISION,
    dist_route_m DOUBLE PRECISION,
    dist_urbain_m DOUBLE PRECISION,

    humidity_min DOUBLE PRECISION,
    temperature_moy DOUBLE PRECISION,
    wind_speed_max DOUBLE PRECISION,
    precipitation_1d_mm DOUBLE PRECISION,
    precipitation_7d_sum_mm DOUBLE PRECISION,
    soil_moisture DOUBLE PRECISION,
    vpd_kpa DOUBLE PRECISION,

    ndvi_30d_mean DOUBLE PRECISION,
    ndvi_30d_min DOUBLE PRECISION,
    ndvi_30d_std DOUBLE PRECISION,

    month INTEGER,
    month_sin DOUBLE PRECISION,
    month_cos DOUBLE PRECISION,
    doy_sin DOUBLE PRECISION,
    doy_cos DOUBLE PRECISION,
    is_weekend INTEGER,

    fires_cell_last_7d INTEGER,
    fires_cell_last_30d INTEGER,
    days_since_last_fire INTEGER,
    seasonal_fire_count INTEGER,
    never_burned_before INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_feature_snapshots_h3_cell
    ON feature_snapshots (h3_cell);

CREATE INDEX IF NOT EXISTS idx_feature_snapshots_event_time
    ON feature_snapshots (event_time);

CREATE INDEX IF NOT EXISTS idx_feature_snapshots_event_date
    ON feature_snapshots (event_date);