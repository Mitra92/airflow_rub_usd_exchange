CREATE TABLE IF NOT EXISTS rub_to_usd (
    base VARCHAR(3) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    rate NUMERIC(12, 3) NOT NULL,
    date DATE NOT NULL,
    UNIQUE (base, currency, date)
);

