import os
import hmac
import hashlib
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

def generate_secure_daily_code(secret_key: str) -> int:
    today = datetime.utcnow().strftime('%Y-%m-%d')
    signature = hmac.new(secret_key.encode(), today.encode(), hashlib.sha256).hexdigest()
    code = int(signature[:8], 16) % 1000000  # 6 chiffres
    return code

def call_post_api():
    secret_key = os.environ.get("SECRET_KEY")
    if not secret_key:
        raise Exception("SECRET_KEY non défini dans l'environnement Docker.")

    code = generate_secure_daily_code(secret_key)

    #url = "https://api.nisu.fr/profil/update_date_conversations_cronjob/"
    url = "172.20.10.4/profil/update_date_conversations_cronjob/"
    payload = {"value": code}
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)

    print("Code sécurisé envoyé :", code)
    print("Status Code:", response.status_code)
    try:
        print("Response JSON:", response.json())
    except Exception:
        print("Response Text:", response.text)

with DAG(
    dag_id="delete_conversation_expired",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["api", "secure"]
) as dag:

    send_secure_code = PythonOperator(
        task_id="send_secure_code",
        python_callable=call_post_api
    )
