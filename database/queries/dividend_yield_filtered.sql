WITH last_price_date AS (
    SELECT
        mp.instrument_id,
        MAX(mp.date) AS last_date
    FROM market_prices mp
    WHERE mp.adj_close IS NOT NULL
    GROUP BY mp.instrument_id
),
last_price AS (
    SELECT
        mp.instrument_id,
        mp.date AS last_date,
        mp.adj_close AS price
    FROM market_prices mp
    JOIN last_price_date l
      ON l.instrument_id = mp.instrument_id
     AND l.last_date = mp.date
),
ttm_div AS (
    SELECT
        lp.instrument_id,
        SUM(mp.dividends) AS ttm_dividends
    FROM last_price lp
    JOIN market_prices mp
      ON mp.instrument_id = lp.instrument_id
     AND mp.date > (lp.last_date - INTERVAL '365 days')
     AND mp.date <= lp.last_date
    GROUP BY lp.instrument_id
),
adv_20 AS (
    SELECT
        lp.instrument_id,
        AVG(mp.volume * mp.adj_close) AS adv_20_usd
    FROM last_price lp
    JOIN market_prices mp
      ON mp.instrument_id = lp.instrument_id
     AND mp.date > (lp.last_date - INTERVAL '20 days')
     AND mp.date <= lp.last_date
    GROUP BY lp.instrument_id
)
SELECT
    i.instrument_id,
    i.ticker,
    lp.last_date,
    lp.price,
    COALESCE(td.ttm_dividends, 0) AS ttm_dividends,
    (COALESCE(td.ttm_dividends, 0) / lp.price) AS ttm_dividend_yield,
    a.adv_20_usd
FROM instruments i
JOIN last_price lp
  ON lp.instrument_id = i.instrument_id
LEFT JOIN ttm_div td
  ON td.instrument_id = i.instrument_id
LEFT JOIN adv_20 a
  ON a.instrument_id = i.instrument_id
WHERE
    -- 1️⃣ 剔除异常高息
    (COALESCE(td.ttm_dividends, 0) / lp.price) <= 0.10

    -- 2️⃣ 剔除仙股
    AND lp.price >= 10

    -- 3️⃣ 剔除低流动性
    AND a.adv_20_usd >= 5_000_000
ORDER BY
    ttm_dividend_yield DESC,
    a.adv_20_usd DESC;
