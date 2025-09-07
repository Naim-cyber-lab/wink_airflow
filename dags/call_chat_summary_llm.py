from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def call_chat_summary(**kwargs):
    try:
        response = requests.get("https://wink-llm.nisu.fr/chat-summary/")
        response.raise_for_status()
        print("✅ Résumé généré :", response.text)
    except Exception as e:
        print(f"❌ Erreur appel /chat-summary/ : {e}")

with DAG(
    dag_id="llm_generate_summary",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",  # chaque jour à 2h
    catchup=False,
    timezone="Europe/Paris",
) as dag:

    generate = PythonOperator(
        task_id="generate_chat_summary",
        python_callable=call_chat_summary,
        provide_context=True
    )
