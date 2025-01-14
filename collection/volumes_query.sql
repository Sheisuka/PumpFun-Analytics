WITH
    buy_txs AS (
        SELECT
            block_time AS "time",
            token_bought_mint_address AS token,
            trader_id AS trader,
            amount_usd,
            tx_id AS tx,
            'buy' AS action
        FROM
            dex_solana.trades trades
        JOIN 
            pumpdotfun_solana.pump_call_create pf
        ON
            pf.account_mint = trades.token_bought_mint_address
        WHERE trades.block_time <= pf.call_block_time + interval '1' hour
        LIMIT 1000
    ),
    sell_txs AS (
        SELECT
            block_time AS "time",
            token_sold_mint_address AS token,
            trader_id AS trader,
            amount_usd,
            tx_id AS tx,
            'sell' AS action
        FROM
            dex_solana.trades trades
        JOIN 
            pumpdotfun_solana.pump_call_create pf
        ON
            pf.account_mint = trades.token_sold_mint_address
        WHERE trades.block_time <= pf.call_block_time + interval '1' hour
        LIMIT 1000
    ),
    buy_data AS (
        SELECT
            token,
            SUM(amount_usd) AS volume,
            COUNT(trader) AS txs
        FROM
            buy_txs
        GROUP BY
            token
    ),
    sell_data AS (
        SELECT
            token,
            SUM(amount_usd) AS volume,
            COUNT(trader) AS txs
        FROM
            sell_txs
        GROUP BY
            token
    )
SELECT 
    b.token,
    (b.volume + COALESCE(s.volume, 0)) AS c_volume,
    (b.txs + COALESCE(s.txs, 0)) AS c_count
FROM 
    buy_data b
LEFT JOIN 
    sell_data s
ON
    b.token = s.token