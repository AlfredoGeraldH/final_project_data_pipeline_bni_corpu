-- Transform: stg_accounts → dim_accounts
-- Cast tipe data, tambah derived columns, deduplikasi

TRUNCATE TABLE dim_accounts;

INSERT INTO dim_accounts (
    account_id,
    account_no,
    account_type,
    product_name,
    currency,
    open_date,
    close_date,
    status,
    interest_rate,
    customer_id,
    branch_id
)
SELECT DISTINCT ON (account_id)
    account_id,
    account_no,
    CASE
        WHEN product_name LIKE '%Tabungan%' THEN 'TABUNGAN'
        WHEN product_name LIKE '%Deposito%' THEN 'DEPOSITO'
        WHEN product_name LIKE '%Giro%' THEN 'GIRO'
        ELSE 'NULL'
    END AS account_type,
    product_name,
    currency,
    open_date::DATE,
    close_date::DATE,
    status,
    interest_rate,
    customer_id,
    branch_id
FROM stg_accounts
WHERE account_id IS NOT NULL
ORDER BY account_id;
