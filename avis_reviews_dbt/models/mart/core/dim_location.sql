WITH locations AS (
    SELECT
        place_id,
        lower(address) AS address,
        latitude,
        longitude
    FROM {{ ref('stg_banque') }}
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['place_id']) }} AS location_id,
    *
FROM locations
