WITH params AS (
    SELECT
        (CURRENT_DATE - INTERVAL '365 days')::date AS d_from,
        CURRENT_DATE::date AS d_to
),
px AS (
    SELECT
        mp.instrument_id,
        mp.date,
        mp.adj_close
    FROM market_prices mp
    JOIN params p ON mp.date BETWEEN p.d_from AND p.d_to
),
latest AS (
    SELECT DISTINCT ON (instrument_id)
        instrument_id,
        adj_close AS latest_adj_close,
        date AS latest_date
    FROM px
    ORDER BY instrument_id, date DESC
),
peak AS (
    SELECT
        instrument_id,
        MAX(adj_close) AS peak_1y
    FROM px
    GROUP BY instrument_id
)
SELECT
    i.ticker,
    i.exchange,
    i.company_name,
    i.sector,
    l.latest_date,
    l.latest_adj_close,
    p.peak_1y,
    (l.latest_adj_close / NULLIF(p.peak_1y, 0) - 1) AS drawdown_from_peak_1y
FROM instruments i
JOIN latest l ON i.instrument_id = l.instrument_id
JOIN peak p   ON i.instrument_id = p.instrument_id
WHERE
    i.is_tradable = TRUE
    AND i.status = 'active'
    AND i.asset_type IN ('Stock','ETF')
    AND p.peak_1y IS NOT NULL
    AND l.latest_adj_close IS NOT NULL
    AND (l.latest_adj_close / NULLIF(p.peak_1y, 0) - 1) <= -0.40  -- 回撤>=40%
ORDER BY
    drawdown_from_peak_1y ASC
LIMIT 200;
