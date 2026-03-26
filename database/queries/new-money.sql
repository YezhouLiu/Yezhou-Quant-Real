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

        LAG(p.adj_close, 10)  OVER (PARTITION BY p.instrument_id ORDER BY p.date) AS px_10d_ago,
        LAG(p.adj_close, 20)  OVER (PARTITION BY p.instrument_id ORDER BY p.date) AS px_20d_ago,
        LAG(p.adj_close, 120) OVER (PARTITION BY p.instrument_id ORDER BY p.date) AS px_120d_ago,

        MAX(p.adj_close) OVER (
            PARTITION BY p.instrument_id
            ORDER BY p.date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS high_20d,

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
    ROUND(((f.adj_close / NULLIF(f.px_10d_ago, 0)  - 1.0) * 100)::numeric, 2) AS ret_10d_pct,
    ROUND(((f.adj_close / NULLIF(f.px_20d_ago, 0)  - 1.0) * 100)::numeric, 2) AS ret_20d_pct,
    ROUND(((f.adj_close / NULLIF(f.px_120d_ago, 0) - 1.0) * 100)::numeric, 2) AS ret_120d_pct,
    ROUND((f.adj_close / NULLIF(f.high_20d, 0))::numeric, 4) AS pct_of_20d_high,
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
    AND i.sector IN ('Technology', 'Industrials')
    AND (f.adj_close / NULLIF(f.px_10d_ago, 0) - 1.0) >= 0.15
    AND (f.adj_close / NULLIF(f.px_20d_ago, 0) - 1.0) >= 0.25
    AND (f.adj_close / NULLIF(f.px_120d_ago, 0) - 1.0) <= 1.20
    AND (f.adj_close / NULLIF(f.high_20d, 0)) >= 0.95
    AND f.adv_20d BETWEEN 3000000 AND 80000000
ORDER BY ret_10d_pct DESC, adv_20d DESC;