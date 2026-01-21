-- Modèle : dim_sentiment.sql
-- Description : Table de dimension des sentiments (positif, négatif, neutre)

WITH sentiments AS (
    SELECT DISTINCT
        lower(sentiment) AS sentiment_label
    FROM {{ ref('stg_avis_clean') }}
    WHERE sentiment IS NOT NULL
),

numbered_sentiments AS (
    SELECT
        row_number() OVER (ORDER BY sentiment_label) AS sentiment_id,
        sentiment_label
    FROM sentiments
)

SELECT * FROM numbered_sentiments

