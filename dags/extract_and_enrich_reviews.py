from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import fasttext
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from gensim import corpora, models
import nltk
import re

nltk.download("vader_lexicon")

# Charger le modèle de langue
ft_model = fasttext.load_model("/home/sabrine123/lid.176.bin")  

analyzer = SentimentIntensityAnalyzer()

def enrich_reviews():
    # 2. Connexion à ma  base PostgreSQL 
    engine = create_engine("postgresql+psycopg2://postgres:motdepasse@localhost:5432/banques_maroc")
  

    df = pd.read_sql("SELECT * FROM avis", engine)

    def detect_lang(text):
        try:
            text = str(text).strip()
            if not text:
                return "unknown"
            return ft_model.predict(text)[0][0].replace("__label__", "")
        except:
            return "unknown"

    def vader_sentiment(text):
        text = str(text).strip()
        if not text:
            return "Neutral"
        score = analyzer.polarity_scores(text)["compound"]
        if score > 0.1:
            return "Positive"
        elif score < -0.1:
            return "Negative"
        else:
            return "Neutral"

    df["langue"] = df["commentaire"].apply(detect_lang)
    df["sentiment"] = df["commentaire"].apply(vader_sentiment)

    texts = df["commentaire"].astype(str).str.lower().apply(lambda x: re.sub(r"[^\w\s]", "", x)).str.split()
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    lda_model = models.LdaModel(corpus=corpus, id2word=dictionary, num_topics=4, passes=10)

    def get_topic_name(text):
        bow = dictionary.doc2bow(text)
        topics = lda_model.get_document_topics(bow)
        return f"Topic {max(topics, key=lambda x: x[1])[0]}" if topics else "Inconnu"

    df["topic"] = [get_topic_name(text) for text in texts]
    df.to_sql("avis_enrichi", engine, if_exists="replace", index=False)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='extract_and_enrich_reviews',
    default_args=default_args,
    schedule='@weekly',
    catchup=False,
    description='Pipeline complet : enrichissement des avis Google'
) as dag:

    enrich_task = PythonOperator(
        task_id='enrich_reviews_task',
        python_callable=enrich_reviews
    )

    enrich_task
