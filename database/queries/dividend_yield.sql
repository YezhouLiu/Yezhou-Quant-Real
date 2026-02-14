WITH last_price_date AS (
    -- 每个标的的最新价格日期（有 adj_close 的那天）
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
        mp.adj_close AS last_adj_close
    FROM market_prices mp
    JOIN last_price_date l
      ON l.instrument_id = mp.instrument_id
     AND l.last_date = mp.date
),
ttm_div AS (
    -- 过去 365 天 TTM 分红（用最后价格日作为锚点往回滚）
    SELECT
        lp.instrument_id,
        SUM(mp.dividends) AS ttm_dividends
    FROM last_price lp
    JOIN market_prices mp
      ON mp.instrument_id = lp.instrument_id
     AND mp.date > (lp.last_date - INTERVAL '365 days')
     AND mp.date <= lp.last_date
    GROUP BY lp.instrument_id
)
SELECT
    i.instrument_id,
    i.ticker,
    lp.last_date,
    lp.last_adj_close AS price,
    COALESCE(td.ttm_dividends, 0) AS ttm_dividends,
    CASE
        WHEN lp.last_adj_close > 0
        THEN COALESCE(td.ttm_dividends, 0) / lp.last_adj_close
        ELSE NULL
    END AS ttm_dividend_yield
FROM instruments i
JOIN last_price lp
  ON lp.instrument_id = i.instrument_id
LEFT JOIN ttm_div td
  ON td.instrument_id = i.instrument_id
ORDER BY ttm_dividend_yield DESC NULLS LAST, i.ticker;
