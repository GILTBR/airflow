CREATE SCHEMA if not exists prod;



CREATE TABLE IF NOT EXISTS prod.usd_exchange_rates
(
    code TEXT,
    rate DOUBLE PRECISION,
    date DATE
);