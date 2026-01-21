WITH source AS (
    SELECT
        avis.id AS review_id,
        avis.note,
        avis.date_review,
        avis.banque_id,
        banque.branch_name,
        avis.sentiment
    FROM {{ ref('stg_avis_clean') }} AS avis
    LEFT JOIN {{ ref('stg_banque') }} AS banque
        ON avis.banque_id = banque.place_id
),

joined AS (
    SELECT
        s.review_id,
        s.note,
        s.date_review,
        b.bank_id,
        br.branch_id,
        loc.location_id,
        se.sentiment_id
    FROM source s
    LEFT JOIN {{ ref('dim_bank') }} b ON s.banque_id = b.bank_id
    LEFT JOIN {{ ref('dim_branch') }} br ON s.branch_name = br.branch_name
    LEFT JOIN {{ ref('dim_location') }} loc ON b.bank_id = loc.location_id
    LEFT JOIN {{ ref('dim_sentiment') }} se ON lower(s.sentiment)= se.sentiment_label
)

SELECT * FROM joined

