from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import fasttext
from gensim import corpora, models
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import sqlalchemy
from sqlalchemy import create_engine
# Téléchargements nécessaires une seule fois
nltk.download("vader_lexicon")

# Paramètres de la base PostgreSQL
engine = create_engine("postgresql+psycopg2://postgres:motdepasse@localhost:5432/banques_maroc")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="eextract_and_enrich_reviews",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule=None,  
    catchup=False
) as dag:

    def extract_reviews():
        df = pd.read_sql("SELECT * FROM avis", engine)
        df.to_csv("/home/sabrine123/avis.csv", index=False)  
        return

    def enrich_reviews():
        df = pd.read_csv("/home/sabrine123/avis.csv")
        df["commentaire"] = df["commentaire"].astype(str)

        # Détection de langue avec fastText
        ft_model = fasttext.load_model("/home/sabrine123/lid.176.bin")
        def detect_lang(text):
            try:
                return ft_model.predict(text)[0][0].replace("__label__", "")
            except:
                return "unknown"
        df["langue"] = df["commentaire"].apply(detect_lang)

        # Sentiment avec VADER
        analyzer = SentimentIntensityAnalyzer()
        def vader_sentiment(text):
            score = analyzer.polarity_scores(text)["compound"]
            if score > 0.1:
                return "Positive"
            elif score < -0.1:
                return "Negative"
            else:
                return "Neutral"
        df["sentiment"] = df["commentaire"].apply(vader_sentiment)

        # Topics (LDA)
        texts = df["commentaire"].str.lower().str.replace(r"[^\w\s]", "", regex=True).str.split()
        dictionary = corpora.Dictionary(texts)
        corpus = [dictionary.doc2bow(text) for text in texts]
        lda_model = models.LdaModel(corpus=corpus, id2word=dictionary, num_topics=4, passes=10, random_state=42)
        def get_topic_name(bow):
            topics = lda_model.get_document_topics(bow)
            if topics:
                return f"Topic {max(topics, key=lambda x: x[1])[0]}"
            else:
                return "Inconnu"
        df["topic"] = [get_topic_name(dictionary.doc2bow(text)) for text in texts]

        df.to_csv("/home/sabrine123/avis_enrichi.csv", index=False)

    def load_to_postgres():
        df = pd.read_csv("/home/sabrine123/avis_enrichi.csv")
        with engine.begin() as conn:
            conn.execute("DELETE FROM avis_enrichi")  # Ne pas DROP pour éviter erreurs
        df.to_sql("avis_enrichi", engine, if_exists="append", index=False)

    extract_reviews_task = PythonOperator(
        task_id="extract_reviews_task",
        python_callable=extract_reviews
    )

    enrich_reviews_task = PythonOperator(
        task_id="enrich_reviews_task",
        python_callable=enrich_reviews
    )

    load_postgres_task = PythonOperator(
        task_id="load_postgres_task",
        python_callable=load_to_postgres
    )

    run_dbt = BashOperator(
    task_id='run_dbt_models',
    bash_command="""
    source ~/dbt-env/bin/activate && \
    cd /mnt/c/Users/Sabri/Documents/avis_banques_dbt && \
    dbt run
    """,
    dag=dag,
    )
    


    extract_reviews_task >> enrich_reviews_task >> load_postgres_task >> run_dbt 
