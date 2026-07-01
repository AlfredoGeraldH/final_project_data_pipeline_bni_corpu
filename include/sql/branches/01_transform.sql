-- Transform: stg_branches → dim_branches
-- Cast tipe data, tambah derived columns, deduplikasi

TRUNCATE TABLE dim_branches;

INSERT INTO dim_branches (
    branch_id,
    branch_code,
    branch_name,
    city,
    province,
    region,
    branch_type,
    open_date,
    is_active
)
SELECT DISTINCT ON (branch_id)
    branch_id,
    branch_code,
    branch_name,
    city,
    province,
    region,
    CASE
        WHEN branch_code LIKE '%KCU%' THEN 'KCU'
        WHEN branch_code LIKE '%KK%' THEN 'KK'
        WHEN branch_code LIKE '%KCP%' THEN 'KCP'
        ELSE 'NULL'
    END AS branch_type,
    open_date::DATE,
    CASE WHEN LOWER(is_active) = 'true' THEN TRUE ELSE FALSE END AS is_active
FROM stg_branches
WHERE branch_id IS NOT NULL
ORDER BY branch_id;
