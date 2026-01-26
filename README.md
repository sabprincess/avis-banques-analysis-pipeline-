# avis-banques-analysis-pipeline-
Projet d'Analyse des Avis Clients Bancaires au Maroc Ce projet utilise une chaîne de traitement automatisée avec Airflow, Python, FastText, LDA et DBT pour enrichir des avis clients avec la langue, le sentiment, et les topics dominants. Les données sont ensuite modélisées en étoile dans PostgreSQL pour des visualisations via Looker Studio.
# 🏦 Data Warehouse & BI – Analyse des avis bancaires

## 🎯 Objectif
Construire une plateforme Data Warehouse permettant d’analyser les avis clients
sur des banques à des fins décisionnelles (qualité de service, satisfaction, tendances).

## 🧱 Architecture
<img width="572" height="163" alt="image" src="https://github.com/user-attachments/assets/9d675d30-1984-40bd-b32b-e891c98ad4c8" />

- Orchestration : Apache Airflow
- Transformation : dbt
- Modélisation : Schéma en étoile
- Stockage : PostgreSQL (Data Warehouse)
- BI : Power BI / Looker / Metabase

## 🔄 Pipeline de données
1. Extraction des avis bancaires (CSV / API)
2. Chargement en zone raw
3. Transformation via dbt
4. Modélisation en étoile
5. Visualisation BI

## 🗂️ Modélisation
- Table de faits : `fact_reviews`
- Dimensions :
  - `dim_bank`
  - `dim_agence`
  - `dim_sentiment`

## 📊 Indicateurs clés
- Score moyen par banque
- Évolution de la satisfaction
- Répartition des sentiments
- Volume d’avis par période

## 🚀 Lancement
```bash
docker-compose up
