WITH cleaned AS (
    SELECT DISTINCT
        place_id,
        lower(regexp_replace(bank_name, '[^\w\s]', '', 'g')) AS bank_name,
        lower(regexp_replace(branch_name, '[^\w\s]', '', 'g')) AS branch_name,
        address,
        latitude,
        longitude
    FROM {{ source('raw_data', 'banquees') }}
    WHERE bank_name IS NOT NULL AND place_id IS NOT NULL
)

SELECT * FROM cleaned

