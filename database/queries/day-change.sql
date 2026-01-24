WITH last_two_dates AS (
    SELECT
        MAX(date) AS d1,
        (
            SELECT MAX(date)
            FROM market_prices
            WHERE date < (SELECT MAX(date) FROM market_prices)
        ) AS d0
    FROM market_prices
),
p1 AS (
    SELECT instrument_id, adj_close AS close1
    FROM market_prices
    WHERE date = (SELECT d1 FROM last_two_dates)
),
p0 AS (
    SELECT instrument_id, adj_close AS close0
    FROM market_prices
    WHERE date = (SELECT d0 FROM last_two_dates)
)
SELECT
    i.instrument_id,
    i.ticker,
    i.company_name,
    i.industry,
    i.exchange,
    (SELECT d0 FROM last_two_dates) AS prev_date,
    (SELECT d1 FROM last_two_dates) AS last_date,
    p0.close0 AS prev_close,
    p1.close1 AS last_close,
    p1.close1 - p0.close0 AS change_amount,
    ROUND(
        (p1.close1 - p0.close0) / p0.close0 * 100,
        4
    ) AS change_pct
FROM p1
JOIN p0 USING (instrument_id)
JOIN instruments i
    ON i.instrument_id = p1.instrument_id
WHERE
    i.status = 'active'
    AND i.asset_type IN ('Stock','ETF')
ORDER BY
    change_pct DESC;
