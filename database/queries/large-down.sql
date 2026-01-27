WITH params AS (
    SELECT
        (CURRENT_DATE - INTERVAL '365 days')::date AS d_from,
        CURRENT_DATE::date AS d_to,
        (CURRENT_DATE - INTERVAL '30 days')::date  AS d_30d,  -- 为了取最近20个交易日左右
        5::numeric AS min_price,
        20000000::numeric AS min_adv20_dollar,     -- 20日平均成交额 >= 2000万
        -0.70::numeric AS dd_min,
        -0.30::numeric AS dd_max
),

px_1y AS (
    SELECT
        mp.instrument_id,
        mp.date,
        mp.adj_close
    FROM market_prices mp, params p
    WHERE mp.date BETWEEN p.d_from AND p.d_to
),

peak AS (
    SELECT instrument_id, MAX(adj_close) AS peak_1y
    FROM px_1y
    GROUP BY instrument_id
),

latest AS (
    SELECT DISTINCT ON (instrument_id)
        instrument_id,
        date AS latest_date,
        adj_close AS latest_adj_close
    FROM px_1y
    ORDER BY instrument_id, date DESC
),

px_30d AS (
    SELECT
        mp.instrument_id,
        mp.adj_close,
        mp.volume,
        (mp.adj_close * mp.volume) AS dollar_volume
    FROM market_prices mp, params p
    WHERE mp.date BETWEEN p.d_30d AND p.d_to
),

adv20 AS (
    SELECT
        instrument_id,
        AVG(dollar_volume) AS adv20_dollar
    FROM px_30d
    WHERE volume IS NOT NULL AND adj_close IS NOT NULL
    GROUP BY instrument_id
)

SELECT
    i.ticker,
    i.exchange,
    i.company_name,
    i.sector,
    i.industry,
    l.latest_date,
    l.latest_adj_close,
    a.adv20_dollar,
    p.peak_1y,
    (l.latest_adj_close / NULLIF(p.peak_1y, 0) - 1) AS drawdown_from_peak_1y
FROM instruments i
JOIN latest l ON i.instrument_id = l.instrument_id
JOIN peak p   ON i.instrument_id = p.instrument_id
JOIN adv20 a  ON i.instrument_id = a.instrument_id
JOIN params par ON TRUE
WHERE
    i.is_tradable = TRUE
    AND i.status = 'active'
    AND i.asset_type = 'Stock'
    AND i.exchange IN ('NYSE','NASDAQ')

    AND l.latest_adj_close >= par.min_price
    AND a.adv20_dollar >= par.min_adv20_dollar

    AND (l.latest_adj_close / NULLIF(p.peak_1y, 0) - 1) BETWEEN par.dd_min AND par.dd_max

    -- ✅ 默认排除Biotech（要保留就把这行注释掉）
    AND NOT (i.sector = 'Healthcare' AND i.industry ILIKE '%biotech%')

    -- ✅ 默认排除明显“风险资产代理/事件股”（要保留就把这行注释掉）
    AND i.ticker NOT IN ('MSTR','MARA','CLSK','DJT')

ORDER BY
    drawdown_from_peak_1y ASC
LIMIT 200;