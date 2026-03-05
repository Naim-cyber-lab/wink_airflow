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

DAG_ID = "updateEventBasePoint"

# Toutes les heures à la minute 30
SCHEDULE_CRON = "30 * * * *"

SQL_UPDATE_EVENT_BASE_POINT = """
WITH computed AS (
  SELECT
    e.id AS event_id,
    (
      SELECT b.id
      FROM profil_basepoint b
      ORDER BY ((b.lat - e.lat)*(b.lat - e.lat) + (b.lon - e.lon)*(b.lon - e.lon)) ASC
      LIMIT 1
    ) AS new_base_point_id
  FROM profil_event e
  WHERE e.lat IS NOT NULL
    AND e.lon IS NOT NULL
)
UPDATE profil_event e
SET base_point_id = computed.new_base_point_id
FROM computed
WHERE e.id = computed.event_id
  AND computed.new_base_point_id IS NOT NULL
  AND e.base_point_id IS DISTINCT FROM computed.new_base_point_id
RETURNING e.id;
"""

def log_updated_count(ti, **_):
    rows = ti.xcom_pull(task_ids="update_event_base_point_task") or []
    updated = len(rows)

    payload = {"updated_events": updated}

    logging.info("=== BasePoint - Event update recap ===")
    logging.info(json.dumps(payload, ensure_ascii=False, indent=2))
    logging.info("=====================================")


with DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_CRON,
    default_args=default_args,
    catchup=False,
    tags=["base_point", "event"],
) as dag:
    dag.timezone = PARIS_TZ

    update_task = PostgresOperator(
        task_id="update_event_base_point_task",
        postgres_conn_id="my_postgres",
        sql=SQL_UPDATE_EVENT_BASE_POINT,
        do_xcom_push=True,
    )

    log_task = PythonOperator(
        task_id="log_updated_count",
        python_callable=log_updated_count,
    )

    update_task >> log_task