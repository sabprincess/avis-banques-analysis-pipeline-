-- models/staging/stg_avis_clean.sql

with ranked as (
    select *,
           row_number() over (
               partition by auteur, commentaire, banque_id, date_review
               order by id
           ) as rang
    from {{ source('raw_data', 'avis_enrichi') }}
),

deduplicated as (
    select *
    from ranked
    where rang = 1
),

normalized as (
    select
        id,
        banque_id,
        auteur,
        lower(regexp_replace(commentaire, '[^\w\s]', '', 'g')) as commentaire_nettoye,
        note,
        date_review,
        langue,
        sentiment,
        topic
    from deduplicated
    where commentaire is not null and commentaire <> ''
)

select * from normalized

