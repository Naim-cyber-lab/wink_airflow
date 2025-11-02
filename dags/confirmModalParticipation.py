from zoneinfo import ZoneInfo
from datetime import datetime
import json
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

PARIS_TZ = ZoneInfo("Europe/Paris")

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

DAG_ID = "insert_confirm_participation_Jmoins2"

# Exécuter chaque jour à 09:00 (Paris)
SCHEDULE_CRON = "0 9 * * *"

# --- SQL idempotent ---
# Insère (winker, conversation) uniquement si pas déjà présent dans profil_confirmparticipationmodal,
# et seulement pour les conversations qui commencent entre maintenant et +2 jours.
SQL_INSERT_CONFIRM_ROWS = """
WITH target_participants AS (
    SELECT
        p."winker_id"         AS winker_id,
        c.id                  AS conversation_id
    FROM profil_conversationactivity c
    JOIN profil_participantconversationactivity p
      ON p."conversationActivity_id" = c.id
    WHERE c."date" >= NOW()
      AND c."date" <  NOW() + INTERVAL '2 days'
),
to_insert AS (
    SELECT tp.winker_id, tp.conversation_id
    FROM target_participants tp
    LEFT JOIN profil_confirmparticipationmodal m
      ON m.winker_id = tp.winker_id
     AND m.conversation_activity_id = tp.conversation_id
    WHERE m.id IS NULL
)
INSERT INTO profil_confirmparticipationmodal
    (winker_id, conversation_activity_id, waiting, confirmed)
SELECT
    winker_id,
    conversation_id,
    TRUE,       -- waiting par défaut
    FALSE       -- confirmed par défaut
FROM to_insert
RETURNING 1;
"""

def log_insert_count(ti, **_):
    """
    Récupère le nombre de lignes insérées (via RETURNING) et logge un récap.
    """
    rows = ti.xcom_pull(task_ids="insert_confirm_rows_task") or []
    inserted = len(rows)  # chaque ligne RETURNING 1 = 1 insertion
    payload = {"inserted_rows": inserted}
    logging.info("=== ConfirmParticipationModal - Insert recap ===")
    logging.info(json.dumps(payload, ensure_ascii=False, indent=2))
    logging.info("================================================")

with DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_CRON,
    default_args=default_args,
    catchup=False,
    tags=["confirm", "participation", "conversations"],
) as dag:
    dag.timezone = PARIS_TZ

    insert_task = PostgresOperator(
        task_id="insert_confirm_rows_task",
        postgres_conn_id="my_postgres",
        sql=SQL_INSERT_CONFIRM_ROWS,
        do_xcom_push=True,  # pour compter les lignes via RETURNING
    )

    log_task = PythonOperator(
        task_id="log_insert_count",
        python_callable=log_insert_count,
    )

    insert_task >> log_task
