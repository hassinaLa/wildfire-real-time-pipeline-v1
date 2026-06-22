CREATE TABLE IF NOT EXISTS predictions (
    id BIGSERIAL PRIMARY KEY,
    h3_cell VARCHAR(32) NOT NULL,
    event_time BIGINT NOT NULL,
    event_date DATE NOT NULL,

    prediction INTEGER NOT NULL,
    probability DOUBLE PRECISION NOT NULL,
    risk_level VARCHAR(16) NOT NULL,
    suspected_voluntary INTEGER DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_predictions_h3_cell
    ON predictions (h3_cell);

CREATE INDEX IF NOT EXISTS idx_predictions_event_time
    ON predictions (event_time);

CREATE INDEX IF NOT EXISTS idx_predictions_event_date
    ON predictions (event_date);

CREATE INDEX IF NOT EXISTS idx_predictions_risk_level
    ON predictions (risk_level);