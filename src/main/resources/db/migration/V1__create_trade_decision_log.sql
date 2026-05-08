CREATE TABLE trade_decision_log (
                                    id BIGSERIAL PRIMARY KEY,
                                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

                                    product_id TEXT NOT NULL,
                                    decision_type TEXT NOT NULL,
                                    reason TEXT NOT NULL,

                                    price NUMERIC(20, 8),
                                    change_24h_percent NUMERIC(10, 4),
                                    usd_available NUMERIC(20, 2),

                                    quote_size_usd NUMERIC(20, 2),
                                    dry_run BOOLEAN NOT NULL,
                                    coinbase_order_id TEXT,
                                    coinbase_success BOOLEAN,
                                    error_message TEXT
);

CREATE INDEX idx_trade_decision_log_created_at
    ON trade_decision_log (created_at);

CREATE INDEX idx_trade_decision_log_daily_buys
    ON trade_decision_log (created_at, decision_type, dry_run);