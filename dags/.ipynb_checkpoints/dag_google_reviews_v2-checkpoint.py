from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import googlemaps
import psycopg2
import json
import os

API_KEY = "AIzaSyARCTUfbIUAUUxFbL6z_ZnB30BVLG5anLw"

default_args = {
    "owner": "airflow",
}

start_date = pendulum.today("UTC").add(days=-1)

with DAG(
    dag_id="dag_google_reviews_v2",
    schedule="@weekly",
    start_date=start_date,
    catchup=False,
    default_args=default_args,
    tags=["banques", "avis", "googlemaps"]
) as dag:

    def extract_reviews(**context):
        gmaps = googlemaps.Client(key=API_KEY)
        avis_extraits = []
        banques_trouvees = []

        BANQUES = [
            "Attijariwafa Bank", "Banque Centrale Populaire", "BMCE Bank of Africa",
            "BMCI", "CFG Bank", "CIH Bank", "Crédit Agricole du Maroc", "Société Générale Maroc"
        ]

        for banque_nom in BANQUES:
            places = gmaps.places(query=banque_nom + " Maroc")

            if "results" in places:
                for place in places["results"]:
                    place_id = place["place_id"]
                    nom_banque = place.get("name")
                    adresse = place.get("formatted_address", "")

                    banques_trouvees.append({
                        "id": place_id,
                        "nom": nom_banque,
                        "location": adresse
                    })

                    details = gmaps.place(place_id=place_id, fields=["reviews"])

                    if "result" in details and "reviews" in details["result"]:
                        for review in details["result"]["reviews"]:
                            avis_extraits.append({
                                "banque_id": place_id,
                                "note": review.get("rating"),
                                "date_review": review.get("time"),
                                "auteur": review.get("author_name"),
                                "commentaire": review.get("text")
                            })

        # 🔹 Gérer le fichier JSON d'historique des avis
        file_path = "/home/sabrine123/airflow/data/avis.json"
        avis_existants = []
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    avis_existants = json.load(f)
                except json.JSONDecodeError:
                    avis_existants = []

        all_avis = avis_existants + avis_extraits

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(all_avis, f, ensure_ascii=False, indent=4)

        print(f"✅ {len(avis_extraits)} nouveaux avis extraits. Total = {len(all_avis)}")

        context['ti'].xcom_push(key="avis", value=avis_extraits)
        context['ti'].xcom_push(key="banques", value=banques_trouvees)

    def load_banques_to_postgres(**context):
        banques = context['ti'].xcom_pull(task_ids="extract_reviews", key="banques")

        if not banques:
            print("Aucune banque à insérer.")
            return

        conn = psycopg2.connect(
            dbname="banques_maroc",
            user="postgres",
            host="localhost",
            port="5433"
        )
        cur = conn.cursor()

        for b in banques:
            cur.execute("SELECT 1 FROM banque WHERE id = %s", (b["id"],))
            if cur.fetchone():
                continue

            cur.execute("""
                INSERT INTO banque (id, nom, location)
                VALUES (%s, %s, %s)
            """, (b["id"], b["nom"], b["location"]))

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Nouvelles banques insérées.")

    def load_to_postgres(**context):
        avis = context['ti'].xcom_pull(task_ids="extract_reviews", key="avis")

        if not avis:
            print("Aucun avis à insérer.")
            return

        conn = psycopg2.connect(
            dbname="banques_maroc",
            user="postgres",
            host="localhost",
            port="5433"
        )
        cur = conn.cursor()

        for review in avis:
            cur.execute("""
                INSERT INTO avis (banque_id, note, date_review, auteur, commentaire)
                VALUES (%s, %s, TO_TIMESTAMP(%s), %s, %s)
            """, (
                review["banque_id"],
                review["note"],
                review["date_review"],
                review["auteur"],
                review["commentaire"]
            ))

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Nouveaux avis insérés.")

    # Tâches
    extract_task = PythonOperator(
        task_id="extract_reviews",
        python_callable=extract_reviews,
        provide_context=True
    )

    load_banques_task = PythonOperator(
        task_id="load_banques_to_postgres",
        python_callable=load_banques_to_postgres,
        provide_context=True
    )

    load_avis_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True
    )

    extract_task >> load_banques_task >> load_avis_task
