--Add time
--Count fresh wallets
--Final price
--Fix COUNTs

WITH
    buy_txs AS (
        SELECT 
            block_time AS "time",
            token_bought_mint_address AS token,
            trader_id AS trader,
            amount_usd,
            tx_id AS tx
        FROM
            dex_solana.trades trades
        JOIN 
            pumpdotfun_solana.pump_call_create pf
        ON
            pf.account_mint = trades.token_bought_mint_address
        WHERE trades.block_time <= pf.call_block_time + interval '1' hour
        LIMIT 30000
    ),
    sell_txs AS (
        SELECT 
            block_time AS "time",
            token_sold_mint_address AS token,
            trader_id AS trader,
            amount_usd,
            tx_id AS tx
        FROM
            dex_solana.trades trades
        JOIN 
            pumpdotfun_solana.pump_call_create pf
        ON
            pf.account_mint = trades.token_sold_mint_address
        WHERE trades.block_time <= pf.call_block_time + interval '1' hour
        LIMIT 30000
    ),
    traders_data AS (
        SELECT 
            token,
            COUNT(DISTINCT trader) AS traders
        FROM (
            SELECT token, trader FROM buy_txs
            UNION
            SELECT token, trader FROM sell_txs
        )
        GROUP BY
            token
    ),
    buy_data AS (
        SELECT
            token,
            SUM(amount_usd) AS volume,
            COUNT(*) AS txs,
            COUNT(DISTINCT trader) as buyers
        FROM
            buy_txs
        GROUP BY
            token
    ),
    sell_data AS (
        SELECT
            token,
            SUM(amount_usd) AS volume,
            COUNT(*) AS txs,
            COUNT(DISTINCT trader) as sellers
        FROM
            sell_txs
        GROUP BY
            token
    )
SELECT 
    b.token AS token,
    b.volume AS buy_volume,
    COALESCE(s.volume, 0) AS sell_volume,
    b.txs AS buy_txs,
    COALESCE(s.txs, 0) AS sell_txs,
    b.buyers AS buyers,
    COALESCE(s.sellers, 0) AS sellers,
    t.traders AS traders
FROM 
    buy_data b
LEFT JOIN 
    sell_data s
ON
    b.token = s.token
JOIN 
    traders_data t
ON
    b.token = t.token