from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def call_relance_conversation(**kwargs):
    try:
        response = requests.get("https://wink-llm.nisu.fr/relance_conversation/")
        response.raise_for_status()
        print("✅ Conversation relancée :", response.text)
    except Exception as e:
        print(f"❌ Erreur appel /relance_conversation/ : {e}")

with DAG(
    dag_id="llm_relance_conversation",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",  # chaque jour à 3h
    catchup=False,
    timezone="Europe/Paris",
) as dag:

    relancer = PythonOperator(
        task_id="relance_conversation",
        python_callable=call_relance_conversation,
        provide_context=True
    )
