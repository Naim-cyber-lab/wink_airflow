import os
import logging
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo

import psycopg2
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ================== CONFIG ==================

PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"  # adapte au nom de ta connexion Postgres

EXPO_API = "https://exp.host/--/api/v2/push/send"
EXPO_ACCESS_TOKEN = Variable.get(
    "EXPO_ACCESS_TOKEN", default_var=os.getenv("EXPO_ACCESS_TOKEN", "")
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ================== HELPERS ==================

def _pg_connect():
    """
    Connexion PostgreSQL via la Connection Airflow `my_postgres`.
    """
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


def _send_expo(tokens, title, body, data=None):
    """
    Envoie des notifications Expo en chunks de 100.
    On filtre les tokens invalides.
    """
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

    headers = {"accept": "application/json", "content-type": "application/json"}
    if EXPO_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {EXPO_ACCESS_TOKEN}"

    sent = 0
    for i in range(0, len(messages), 100):
        chunk = messages[i: i + 100]
        try:
            r = requests.post(EXPO_API, json=chunk, headers=headers, timeout=15)
            r.raise_for_status()
            res = r.json()
            data_items = res.get("data") or []
            errors = [
                d for d in data_items
                if isinstance(d, dict) and d.get("status") != "ok"
            ]
            if errors:
                logging.warning("Expo push errors: %s", errors)
            sent += len(chunk)
        except Exception as e:
            logging.exception("Expo push failed on chunk: %s", e)
    return sent


# ================== CORE TASK ==================

def send_conversation_activity_reminders(**kwargs):
    """
    Envoie des notifications push aux participants (waiting = false)
    des ConversationActivity 2h, 10h, 24h et 48h avant l'heure (`date`).
    AUCUN log en base : on se base seulement sur les fenêtres de temps.
    """
    now = datetime.now(PARIS_TZ)

    # Fenêtre = fréquence du DAG (ici 10 minutes)
    window_minutes = 10

    delays = [
        (24, "24h"),
    ]

    with _pg_connect() as connection:
        with connection.cursor() as cur:
            for hours_before, label in delays:
                window_start = now + timedelta(hours=hours_before)
                window_end = window_start + timedelta(minutes=window_minutes)

                logging.info(
                    "⏰ Recherche des ConversationActivity pour rappel %s "
                    "entre %s et %s",
                    label, window_start, window_end
                )

                # Tables Django :
                #  - ConversationActivity              -> profil_conversationactivity
                #  - ParticipantConversationActivity   -> profil_participantconversationactivity
                #  - Winker                            -> profil_winker
                #
                # Champs à majuscule/camelCase à QUOTER :
                #  - ConversationActivity.date         -> "date"
                #  - ParticipantConversationActivity.conversationActivity_id -> "conversationActivity_id"
                #  - Winker.expoPushToken              -> "expoPushToken"

                cur.execute(
                    """
                    WITH candidate_conversations AS (
                        SELECT ca.id, ca."date", ca.title
                        FROM profil_conversationactivity ca
                        WHERE ca.is_deleted = false
                          AND ca."date" >= %s
                          AND ca."date" <  %s
                    )
                    SELECT
                        ca.id AS conversation_id,
                        ca.title AS conversation_title,
                        array_agg(w."expoPushToken") AS tokens
                    FROM candidate_conversations ca
                    JOIN profil_participantconversationactivity p
                      ON p."conversationActivity_id" = ca.id
                     AND p.waiting = false
                    JOIN profil_winker w
                      ON w.id = p.winker_id
                    WHERE w."expoPushToken" IS NOT NULL
                    GROUP BY ca.id, ca.title
                    """,
                    (window_start, window_end),
                )
                rows = cur.fetchall()

                if not rows:
                    logging.info("Aucune conversation à notifier pour %s.", label)
                    continue

                for conversation_id, conversation_title, tokens in rows:
                    if not tokens:
                        continue

                    title = "Rappel de rencontre 📆"
                    body = (
                        f"Rappel : la rencontre \"{conversation_title}\" "
                        f"commence dans {hours_before}h."
                    )
                    payload = {
                        "type": "conversation_activity_reminder",
                        "conversation_id": conversation_id,
                        "hours_before": hours_before,
                    }

                    sent = _send_expo(tokens, title, body, data=payload)
                    logging.info(
                        "📲 %s notifications envoyées pour ConversationActivity %s (%s avant).",
                        sent, conversation_id, label
                    )


# ================== DAG ==================

with DAG(
    dag_id="conversation_activity_reminders",
    description=(
        "Envoie des notifications de rappel aux participants des ConversationActivity "
        "2h, 10h, 24h et 48h avant le début."
    ),
    default_args=default_args,
    schedule_interval="*/10 * * * *",  # toutes les 10 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["conversation", "reminder", "expo"],
) as dag:
    dag.timezone = PARIS_TZ

    send_reminders = PythonOperator(
        task_id="send_conversation_activity_reminders",
        python_callable=send_conversation_activity_reminders,
        provide_context=True,
    )
