from zoneinfo import ZoneInfo
from datetime import datetime
import json
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

# Fuseau horaire Paris
PARIS_TZ = ZoneInfo("Europe/Paris")

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# Nom du DAG
DAG_ID = "confirmModalParticipation"

# Planification : toutes les heures à la minute 0
# 00:00, 01:00, 02:00, ...
SCHEDULE_CRON = "0 * * * *"

# SQL d'insertion :
# - On prend toutes les ConversationActivity qui commencent dans les 48h
# - On prend leurs participants
# - On insère dans profil_confirmparticipationmodal seulement si l'entrée n'existe pas déjà
# - created_at est rempli avec NOW() pour respecter le NOT NULL
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
    (winker_id, conversation_activity_id, waiting, confirmed, created_at)
SELECT
    winker_id,
    conversation_id,
    TRUE,       -- waiting par défaut
    FALSE,      -- confirmed par défaut
    NOW()       -- created_at non null
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
    # On force le fuseau pour le DAG
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

    # Ordre d'exécution
    insert_task >> log_task
