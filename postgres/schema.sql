CREATE SCHEMA IF NOT EXISTS financial_market;

CREATE TABLE IF NOT EXISTS financial_market.market_anomalies (
    symbol text NOT NULL,
    trade_date date NOT NULL,
    close double precision NOT NULL,
    previous_close double precision,
    daily_return double precision,
    prior_days integer NOT NULL,
    baseline_volatility double precision,
    threshold double precision NOT NULL,
    is_unusual boolean NOT NULL,
    PRIMARY KEY (symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS financial_market.economic_observations (
    series_id text NOT NULL,
    observation_date date NOT NULL,
    value double precision NOT NULL,
    realtime_start date,
    realtime_end date,
    PRIMARY KEY (series_id, observation_date)
);
