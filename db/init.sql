CREATE TABLE IF NOT EXISTS scores (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag SMALLINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scores_fraud_flag ON scores (fraud_flag, id DESC);
