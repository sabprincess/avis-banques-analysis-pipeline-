WITH branches AS (
    SELECT
        place_id AS branch_id,
        place_id AS bank_id,  -- si place_id sert pour les deux
        lower(regexp_replace(branch_name, '[^\w\s]', '', 'g')) AS branch_name
    FROM {{ ref('stg_banque') }}
    WHERE branch_name IS NOT NULL
)

SELECT * FROM branches
