--branch
SELECT
    b.branch_id,
    b.branch_name,
    b.region,
    COUNT(f.transaction_id)
        AS transaction_volume,
    SUM(f.amount)
        AS total_transaction_value
FROM fact_transactions f
JOIN dim_branches b
    ON f.branch_id = b.branch_id
GROUP BY
    b.branch_id,
    b.branch_name,
    b.region
ORDER BY
    total_transaction_value DESC;

--Region
SELECT
    b.region,
    COUNT(f.transaction_id)
        AS transaction_volume,
    SUM(f.amount)
        AS total_value
FROM fact_transactions f
JOIN dim_branches b
    ON f.branch_id = b.branch_id
GROUP BY b.region
ORDER BY total_value DESC;