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

DAG_ID = "updateWinkerBasePoint"

# Toutes les heures à la minute 10
SCHEDULE_CRON = "10 * * * *"

# Met à jour uniquement les winkers qui n'ont pas encore de base_point_id
# + qui ont lat/lon
SQL_UPDATE_WINKER_BASE_POINT = """
WITH computed AS (
  SELECT
    w.id AS winker_id,
    (
      SELECT b.id
      FROM profil_basepoint b
      ORDER BY ((b.lat - w.lat)*(b.lat - w.lat) + (b.lon - w.lon)*(b.lon - w.lon)) ASC
      LIMIT 1
    ) AS new_base_point_id
  FROM profil_winker w
  WHERE w.lat IS NOT NULL
    AND w.lon IS NOT NULL
)
UPDATE profil_winker w
SET base_point_id = computed.new_base_point_id
FROM computed
WHERE w.id = computed.winker_id
  AND computed.new_base_point_id IS NOT NULL
  AND w.base_point_id IS DISTINCT FROM computed.new_base_point_id
RETURNING w.id;

"""

def log_updated_count(ti, **_):
    rows = ti.xcom_pull(task_ids="update_winker_base_point_task") or []
    updated = len(rows)  # chaque ligne RETURNING = 1 winker update
    payload = {"updated_winkers": updated}
    logging.info("=== BasePoint - Winker update recap ===")
    logging.info(json.dumps(payload, ensure_ascii=False, indent=2))
    logging.info("======================================")

with DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_CRON,
    default_args=default_args,
    catchup=False,
    tags=["base_point", "winker"],
) as dag:
    dag.timezone = PARIS_TZ

    update_task = PostgresOperator(
        task_id="update_winker_base_point_task",
        postgres_conn_id="my_postgres",
        sql=SQL_UPDATE_WINKER_BASE_POINT,
        do_xcom_push=True,
    )

    log_task = PythonOperator(
        task_id="log_updated_count",
        python_callable=log_updated_count,
    )

    update_task >> log_task
