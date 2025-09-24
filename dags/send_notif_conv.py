# dags/notify_idf_new_conversations.py
import os
import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

import psycopg2
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ---------- CONFIG ----------
DB_CONN_ID = "my_postgres"  # Airflow Connection -> Postgres
PARIS_TZ = ZoneInfo("Europe/Paris")

# Expo Push
EXPO_API = "https://exp.host/--/api/v2/push/send"
# Configure en Variable Airflow (Admin -> Variables) ou en env
EXPO_ACCESS_TOKEN = Variable.get("EXPO_ACCESS_TOKEN", default_var=os.getenv("EXPO_ACCESS_TOKEN", ""))

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------- HELPERS SQL / EXPO ----------

def _pg_connect():
    """Connexion Postgres via Airflow Connection."""
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )

def _send_expo(tokens, title, body, data=None):
    """Envoie des push Expo par paquets de 100."""
    # Filtrer les tokens valides Expo
    messages = [
        {
            "to": t,
            "title": title,
            "body": body,
            "sound": "default",
            "data": data or {},
            "priority": "high",
        }
        for t in tokens
        if t and isinstance(t, str) and t.startswith("ExponentPushToken")
    ]
    if not messages:
        logging.info("🔕 Aucun token Expo valide.")
        return 0

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
    }
    if EXPO_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {EXPO_ACCESS_TOKEN}"

    sent = 0
    for i in range(0, len(messages), 100):
        chunk = messages[i : i + 100]
        try:
            r = requests.post(EXPO_API, json=chunk, headers=headers, timeout=15)
            r.raise_for_status()
            res = r.json()
            # Log des erreurs renvoyées par Expo
            try:
                data_items = res.get("data") or []
                errors = [d for d in data_items if isinstance(d, dict) and d.get("status") != "ok"]
                if errors:
                    logging.warning("Expo push errors: %s", errors)
            except Exception:
                pass
            sent += len(chunk)
        except Exception as e:
            logging.exception("Expo push failed on chunk: %s", e)
    return sent

# ---------- TASKS ----------

def get_idf_tokens(**kwargs):
    """
    Récupère les expoPushToken des Winkers dont la région correspond à l'Île-de-France.
    Variantes acceptées (insensible à la casse) :
    'ile-de-france', 'ile de france', 'ile_de_france', 'idf'.
    """
    ti = kwargs["ti"]
    with _pg_connect() as connection, connection.cursor() as cur:
        cur.execute(
            """
            SELECT "expoPushToken"
            FROM profil_winker
            WHERE LOWER(COALESCE(region, '')) IN (
                'ile-de-france', 'ile de france', 'ile_de_france', 'idf', 'Île-de-France'
            )
              AND "expoPushToken" IS NOT NULL
              AND "expoPushToken" <> ''
            """
        )
        tokens = [row[0] for row in cur.fetchall()]
    logging.info("👥 %s tokens Expo trouvés pour Île-de-France.", len(tokens))
    ti.xcom_push(key="idf_tokens", value=tokens)

def push_fixed_message(**kwargs):
    """
    Envoie le push avec le message EXACT demandé :
    "14 nouvelles conversations disponible !"
    """
    ti = kwargs["ti"]
    tokens = ti.xcom_pull(task_ids="get_idf_tokens", key="idf_tokens") or []
    if not tokens:
        logging.info("🔕 Aucun token à notifier.")
        return

    title = "Nouvelles conversations 📣"  # peut être ajusté si tu veux le laisser vide
    body = "14 nouvelles conversations disponible !"  # NE PAS MODIFIER (exigence utilisateur)
    payload = {"type": "conversation_info", "region": "ile-de-france", "fixed_count": 14}

    sent = _send_expo(tokens, title, body, data=payload)
    logging.info("📲 Push envoyé à %s utilisateurs (Île-de-France).", sent)

# ---------- DAG ----------

with DAG(
    dag_id="notify_idf_new_conversations",
    default_args=default_args,
    description="Envoie une notification fixe pour l'Île-de-France (Expo).",
    schedule_interval="0 7 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["notifications", "expo", "idf"],
) as dag:
    dag.timezone = PARIS_TZ

    t1 = PythonOperator(
        task_id="get_idf_tokens",
        python_callable=get_idf_tokens,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="send_push",
        python_callable=push_fixed_message,
        provide_context=True,
    )

    t1 >> t2
