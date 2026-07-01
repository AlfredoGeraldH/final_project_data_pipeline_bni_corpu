--Top Active Customer
SELECT
    c.customer_id,
    c.full_name,
    c.segment,
    COUNT(f.transaction_id)
        AS transaction_frequency,
    SUM(f.amount)
        AS total_transaction_value
FROM fact_transactions f
JOIN dim_customers c
    ON f.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.full_name,
    c.segment
ORDER BY
    total_transaction_value DESC;

--Segment Distribution
SELECT
    c.segment,
    COUNT(DISTINCT c.customer_id)
        AS total_customer,
    COUNT(f.transaction_id)
        AS transaction_volume,
    SUM(f.amount)
        AS transaction_value
FROM dim_customers c
LEFT JOIN fact_transactions f
    ON c.customer_id = f.customer_id
GROUP BY c.segment
ORDER BY transaction_value DESC;