# dags/update_conversations_for_today.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import psycopg2
import logging

DB_CONN_ID = "my_postgres"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

DAG_DOC = r"""
# 🔁 Rotation des **conversations** pour **aujourd'hui** (par région)

Objectif
- Garantir un nombre cible de conversations **aujourd'hui** par région (targets dans `profil_nisu_param_config`, `perimeter='conversation'`).
- Si une région est en dessous du quota, on prend les **plus récentes** en priorité et on met `datePublication = aujourd'hui`.
"""

def _pg_connect():
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
    )

def get_targets(**kwargs):
    ti = kwargs["ti"]
    with _pg_connect() as connection, connection.cursor() as cur:
        cur.execute("""
            SELECT config_one AS region, float_param::int AS target
            FROM profil_nisu_param_config
            WHERE perimeter = 'conversation'
        """)
        targets = {r: t for (r, t) in cur.fetchall()}
    logging.info("🎯 Targets (conversation): %s", targets)
    ti.xcom_push(key="targets", value=targets)

def get_published_today(**kwargs):
    ti = kwargs["ti"]
    today = datetime.now().date()  # si tu préfères le fuseau DB: fais COUNT avec CURRENT_DATE côté SQL
    with _pg_connect() as connection, connection.cursor() as cur:
        cur.execute("""
            SELECT t.config_one AS region, COALESCE(COUNT(c.*), 0) AS cnt
            FROM profil_nisu_param_config t
            LEFT JOIN profil_conversationactivity c
              ON c.region = t.config_one
             AND c."datePublication" = %s
            WHERE t.perimeter = 'conversation'
            GROUP BY t.config_one
            ORDER BY t.config_one
        """, (today,))
        published = {r: int(cnt) for (r, cnt) in cur.fetchall()}
    logging.info("📦 Déjà planifiées pour aujourd'hui: %s", published)
    ti.xcom_push(key="published", value=published)

def compute_missing(**kwargs):
    ti = kwargs["ti"]
    targets = ti.xcom_pull(task_ids="get_targets", key="targets") or {}
    published = ti.xcom_pull(task_ids="get_published_today", key="published") or {}
    missing = {}
    for region, target in targets.items():
        cur = published.get(region, 0)
        need = max(int(target) - int(cur), 0)
        if need > 0:
            missing[region] = need
    logging.info("🧮 Manque par région: %s", missing)
    ti.xcom_push(key="missing", value=missing)

def rotate_and_update_for_today(**kwargs):
    """Assigne datePublication=AUJOURD'HUI aux dernières conversations (roulement)."""
    ti = kwargs["ti"]
    missing = ti.xcom_pull(task_ids="compute_missing", key="missing") or {}
    if not missing:
        logging.info("✅ Aucun complément nécessaire.")
        return

    today = datetime.now().date()

    with _pg_connect() as connection, connection.cursor() as cur:
        total = 0
        for region, to_add in missing.items():
            logging.info("↪️ Région %s : besoin de %s", region, to_add)

            # Candidats = même région, pas déjà aujourd'hui
            # Priorité aux plus RÉCENTES : datePublication DESC (NULLS FIRST), id DESC
            cur.execute("""
                SELECT id
                FROM profil_conversationactivity
                WHERE region = %s
                  AND ("datePublication" IS DISTINCT FROM %s)
                ORDER BY "datePublication" DESC NULLS FIRST, id DESC
                LIMIT %s
            """, (region, today, to_add))
            ids = [row[0] for row in cur.fetchall()]
            if not ids:
                logging.info("… Pas de candidats pour %s", region)
                continue

            cur.execute("""
                UPDATE profil_conversationactivity
                SET "datePublication" = %s
                WHERE id = ANY(%s)
            """, (today, ids))
            total += cur.rowcount
            logging.info("✅ %s conversations fixées à aujourd'hui (%s)", cur.rowcount, region)

        connection.commit()
        logging.info("🎉 Total mises à aujourd'hui: %s", total)

with DAG(
    dag_id="update_conversations_for_today",
    start_date=days_ago(1),
    schedule_interval="0 1 * * *",  # tous les jours à 01:00 (à adapter si besoin)
    catchup=False,
    default_args=default_args,
    tags=["conversation", "region", "rotation", "quota"],
) as dag:
    dag.doc_md = DAG_DOC

    t1 = PythonOperator(
        task_id="get_targets",
        python_callable=get_targets,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="get_published_today",
        python_callable=get_published_today,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="compute_missing",
        python_callable=compute_missing,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="rotate_and_update_for_today",
        python_callable=rotate_and_update_for_today,
        provide_context=True,
    )

    [t1, t2] >> t3 >> t4
