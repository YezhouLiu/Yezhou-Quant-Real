WITH params AS (
    SELECT
        CURRENT_DATE::date AS d0,
        (CURRENT_DATE - INTERVAL '365 days')::date AS d_1y,
        (CURRENT_DATE - INTERVAL '63 days')::date  AS d_3m,   -- 约3个月交易日
        (CURRENT_DATE - INTERVAL '21 days')::date  AS d_1m    -- 约1个月交易日
),
px AS (
    SELECT mp.instrument_id, mp.date, mp.adj_close
    FROM market_prices mp
    JOIN params p ON mp.date BETWEEN p.d_1y AND p.d0
),
latest AS (
    SELECT DISTINCT ON (instrument_id)
        instrument_id, adj_close AS px_now
    FROM px
    ORDER BY instrument_id, date DESC
),
px_1m AS (
    SELECT DISTINCT ON (instrument_id)
        instrument_id, adj_close AS px_1m
    FROM px
    JOIN params p ON date <= p.d_1m
    ORDER BY instrument_id, date DESC
),
px_3m AS (
    SELECT DISTINCT ON (instrument_id)
        instrument_id, adj_close AS px_3m
    FROM px
    JOIN params p ON date <= p.d_3m
    ORDER BY instrument_id, date DESC
),
peak_1y AS (
    SELECT instrument_id, MAX(adj_close) AS peak_1y
    FROM px
    GROUP BY instrument_id
)
SELECT
    i.ticker,
    i.company_name,
    i.sector,
    l.px_now,
    p1m.px_1m,
    p3m.px_3m,
    pk.peak_1y,
    (l.px_now / NULLIF(pk.peak_1y, 0) - 1) AS dd_1y,
    (l.px_now / NULLIF(p1m.px_1m, 0) - 1) AS ret_1m,
    (l.px_now / NULLIF(p3m.px_3m, 0) - 1) AS ret_3m
FROM instruments i
JOIN latest l   ON i.instrument_id = l.instrument_id
JOIN px_1m p1m  ON i.instrument_id = p1m.instrument_id
JOIN px_3m p3m  ON i.instrument_id = p3m.instrument_id
JOIN peak_1y pk ON i.instrument_id = pk.instrument_id
WHERE
    i.is_tradable = TRUE
    AND i.status = 'active'
    AND i.asset_type = 'Stock'
    AND (l.px_now / NULLIF(pk.peak_1y, 0) - 1) <= -0.35  -- 1年回撤>=35%
    AND (l.px_now / NULLIF(p1m.px_1m, 0) - 1) >= 0.10   -- 近1月反弹>=10%
ORDER BY
    ret_1m DESC,
    dd_1y ASC
LIMIT 200;
