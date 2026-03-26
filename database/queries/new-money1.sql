WITH px AS (
    SELECT
        mp.instrument_id,
        mp.date,
        mp.adj_close,
        mp.adj_volume,
        (mp.adj_close * mp.adj_volume)::numeric AS dollar_volume
    FROM market_prices mp
    WHERE mp.adj_close IS NOT NULL
),
feat AS (
    SELECT
        p.instrument_id,
        p.date,
        p.adj_close,
        p.dollar_volume,

        LAG(p.adj_close, 20)  OVER (PARTITION BY p.instrument_id ORDER BY p.date) AS px_20d_ago,
        LAG(p.adj_close, 40)  OVER (PARTITION BY p.instrument_id ORDER BY p.date) AS px_40d_ago,
        LAG(p.adj_close, 60)  OVER (PARTITION BY p.instrument_id ORDER BY p.date) AS px_60d_ago,
        LAG(p.adj_close, 120) OVER (PARTITION BY p.instrument_id ORDER BY p.date) AS px_120d_ago,

        MAX(p.adj_close) OVER (
            PARTITION BY p.instrument_id
            ORDER BY p.date
            ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
        ) AS high_40d,

        AVG(p.dollar_volume) OVER (
            PARTITION BY p.instrument_id
            ORDER BY p.date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS adv_20d
    FROM px p
),
latest AS (
    SELECT MAX(date) AS asof_date
    FROM market_prices
)
SELECT
    i.ticker,
    i.company_name,
    i.sector,
    i.industry,
    f.date,
    f.adj_close,
    ROUND(((f.adj_close / NULLIF(f.px_20d_ago, 0)  - 1.0) * 100)::numeric, 2) AS ret_20d_pct,
    ROUND(((f.adj_close / NULLIF(f.px_40d_ago, 0)  - 1.0) * 100)::numeric, 2) AS ret_40d_pct,
    ROUND(((f.adj_close / NULLIF(f.px_60d_ago, 0)  - 1.0) * 100)::numeric, 2) AS ret_60d_pct,
    ROUND(((f.adj_close / NULLIF(f.px_120d_ago, 0) - 1.0) * 100)::numeric, 2) AS ret_120d_pct,
    ROUND((f.adj_close / NULLIF(f.high_40d, 0))::numeric, 4) AS pct_of_40d_high,
    ROUND(f.adv_20d::numeric, 0) AS adv_20d
FROM feat f
JOIN latest l
  ON f.date = l.asof_date
JOIN instruments i
  ON i.instrument_id = f.instrument_id
WHERE
    i.asset_type = 'Stock'
    AND i.status = 'active'
    AND i.is_tradable = TRUE
    AND i.industry IN (
        'Semiconductors',
        'Semiconductor Equipment & Materials',
        'Communication Equipment',
        'Computer Hardware',
        'Electronic Components',
        'Software - Infrastructure',
        'Information Technology Services',
        'Scientific & Technical Instruments',
        'Electrical Equipment & Parts',
        'Specialty Industrial Machinery'
    )
    AND (f.adj_close / NULLIF(f.px_20d_ago, 0) - 1.0) > 0
    AND (f.adj_close / NULLIF(f.px_40d_ago, 0) - 1.0) > 0
    AND (f.adj_close / NULLIF(f.px_60d_ago, 0) - 1.0) > 0
    AND (f.adj_close / NULLIF(f.px_120d_ago, 0) - 1.0) <= 1.20
    AND (f.adj_close / NULLIF(f.high_40d, 0)) >= 0.88
    AND f.adv_20d BETWEEN 2000000 AND 100000000
ORDER BY
    ((f.adj_close / NULLIF(f.px_40d_ago, 0) - 1.0)) DESC,
    ((f.adj_close / NULLIF(f.px_20d_ago, 0) - 1.0)) DESC;