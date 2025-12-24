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

DAG_ID = "updateConversationActivityBasePoint"

# Toutes les heures à la minute 20
SCHEDULE_CRON = "20 * * * *"

# Attention : ConversationActivity utilise lat / lng (pas lon)
# On ignore les conversations supprimées si tu as is_deleted
SQL_UPDATE_CONV_BASE_POINT = """
WITH computed AS (
  SELECT
    c.id AS conversation_id,
    (
      SELECT b.id
      FROM profil_basepoint b
      ORDER BY ((b.lat - c.lat)*(b.lat - c.lat) + (b.lon - c.lng)*(b.lon - c.lng)) ASC
      LIMIT 1
    ) AS new_base_point_id
  FROM profil_conversationactivity c
  WHERE c.lat IS NOT NULL
    AND c.lng IS NOT NULL
    AND (c.is_deleted IS NULL OR c.is_deleted = FALSE)
)
UPDATE profil_conversationactivity c
SET base_point_id = computed.new_base_point_id
FROM computed
WHERE c.id = computed.conversation_id
  AND computed.new_base_point_id IS NOT NULL
  AND c.base_point_id IS DISTINCT FROM computed.new_base_point_id
RETURNING c.id;
"""

def log_updated_count(ti, **_):
    rows = ti.xcom_pull(task_ids="update_conversation_base_point_task") or []
    updated = len(rows)
    payload = {"updated_conversations": updated}
    logging.info("=== BasePoint - ConversationActivity update recap ===")
    logging.info(json.dumps(payload, ensure_ascii=False, indent=2))
    logging.info("====================================================")

with DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_CRON,
    default_args=default_args,
    catchup=False,
    tags=["base_point", "conversation"],
) as dag:
    dag.timezone = PARIS_TZ

    update_task = PostgresOperator(
        task_id="update_conversation_base_point_task",
        postgres_conn_id="my_postgres",
        sql=SQL_UPDATE_CONV_BASE_POINT,
        do_xcom_push=True,
    )

    log_task = PythonOperator(
        task_id="log_updated_count",
        python_callable=log_updated_count,
    )

    update_task >> log_task
