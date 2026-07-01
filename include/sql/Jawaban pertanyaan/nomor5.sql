SELECT
    a.product_name,
    a.account_type,
    COUNT(f.transaction_id)
        AS transaction_volume,
    SUM(f.amount)
        AS total_transaction_value,
    AVG(f.balance_after)
        AS average_balance
FROM fact_transactions f
JOIN dim_accounts a
    ON f.account_id = a.account_id
GROUP BY
    a.product_name,
    a.account_type
ORDER BY
    total_transaction_value DESC;

