-- models/marts/core/dim_bank.sql
WITH cleaned AS (
    SELECT
        place_id AS bank_id,
        lower(regexp_replace(bank_name, '[^\w\s]', '', 'g')) AS bank_name,
        lower(regexp_replace(branch_name, '[^\w\s]', '', 'g')) AS branch_name
    FROM {{ ref('stg_banque') }}
    WHERE bank_name IS NOT NULL
)

SELECT * FROM cleaned

