WITH 
    creations AS (
        SELECT 
            DATE_TRUNC('day', call_block_date) AS "date",
            call_block_time AS date_time,
            account_mint AS contract_address,
            name,
            symbol
        FROM pumpdotfun_solana.pump_call_create
    ),
    days AS (
        SELECT 
            "date",
            COUNT(contract_address) AS daily_creations
        FROM creations
        GROUP BY "date"
        ORDER BY date ASC
    )
SELECT
    *,
    SUM(daily_creations) OVER(ORDER BY "date" ASC) AS total_creations
FROM days