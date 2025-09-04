# dags/update_conversations_for_next_day.py
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
# 🔁 Rotation des **conversations** pour J+1 (par région)

**Objectif**
- Pour **demain**, garantir un nombre cible de conversations publiées **par région** (targets dans `profil_nisu_param_config`, `perimeter='conversation'`).
- Si une région est en dessous du quota, on repousse **les conversations les plus récentes** (priorité aux dernières, *roulement*) en mettant `datePublication = demain`.

**Tables**
- `profil_nisu_param_config` (targets par région)
  - `perimeter='conversation'`, `config_one` = région, `float_param` = quota
- `profil_conversationactivity`
  - champs utilisés : `id`, `region`, `"datePublication"`

**Plan**
1) Lire les **targets** par région.
2) Compter les conversations déjà planifiées pour **demain**.
3) Calculer le **manque** `max(target - count, 0)`.
4) Pour chaque région manquante, **réassigner** `datePublication = demain` aux conversations candidates, en **priorisant les plus récentes**:
   - candidates: même région
   - exclure celles déjà prévues demain
   - ordre: `ORDER BY "datePublication" DESC NULLS FIRST, id DESC`
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
    """targets = {region: quota_int} pour perimeter='conversation'"""
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

def get_published_tomorrow(**kwargs):
    """published = {region: count} pour les conversations déjà prévues demain"""
    ti = kwargs["ti"]
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    with _pg_connect() as connection, connection.cursor() as cur:
        # Compte avec LEFT JOIN pour avoir toutes les régions cibles, même à 0
        cur.execute("""
            SELECT t.config_one AS region, COALESCE(COUNT(c.*), 0) AS cnt
            FROM profil_nisu_param_config t
            LEFT JOIN profil_conversationactivity c
              ON c.region = t.config_one
             AND c."datePublication" = %s
            WHERE t.perimeter = 'conversation'
            GROUP BY t.config_one
            ORDER BY t.config_one
        """, (tomorrow,))
        published = {r: int(cnt) for (r, cnt) in cur.fetchall()}
    logging.info("📦 Déjà planifiées pour demain: %s", published)
    ti.xcom_push(key="published", value=published)

def compute_missing(**kwargs):
    """missing = {region: to_add}"""
    ti = kwargs["ti"]
    targets = ti.xcom_pull(task_ids="get_targets", key="targets") or {}
    published = ti.xcom_pull(task_ids="get_published_tomorrow", key="published") or {}
    missing = {}
    for region, target in targets.items():
        cur = published.get(region, 0)
        need = max(int(target) - int(cur), 0)
        if need > 0:
            missing[region] = need
    logging.info("🧮 Manque par région: %s", missing)
    ti.xcom_push(key="missing", value=missing)

def rotate_and_update(**kwargs):
    """Assigne datePublication=demain aux dernières conversations (roulement)."""
    ti = kwargs["ti"]
    missing = ti.xcom_pull(task_ids="compute_missing", key="missing") or {}
    if not missing:
        logging.info("✅ Aucun complément nécessaire.")
        return

    tomorrow = (datetime.now() + timedelta(days=1)).date()

    with _pg_connect() as connection, connection.cursor() as cur:
        total = 0
        for region, to_add in missing.items():
            logging.info("↪️ Région %s : besoin de %s", region, to_add)

            # Candidats = même région, pas déjà demain
            # Priorité aux plus RÉCENTES (roulement) : datePublication DESC (NULLS FIRST), id DESC
            cur.execute("""
                SELECT id
                FROM profil_conversationactivity
                WHERE region = %s
                  AND ("datePublication" IS DISTINCT FROM %s)
                ORDER BY "datePublication" DESC NULLS FIRST, id DESC
                LIMIT %s
            """, (region, tomorrow, to_add))
            ids = [row[0] for row in cur.fetchall()]
            if not ids:
                logging.info("… Pas de candidats pour %s", region)
                continue

            # Mise à jour en lot
            cur.execute("""
                UPDATE profil_conversationactivity
                SET "datePublication" = %s
                WHERE id = ANY(%s)
            """, (tomorrow, ids))
            total += cur.rowcount
            logging.info("✅ %s conversations replanifiées pour demain (%s)", cur.rowcount, region)

        connection.commit()
        logging.info("🎉 Total replanifiées: %s", total)

with DAG(
    dag_id="update_conversations_for_next_day",
    start_date=days_ago(1),
    schedule_interval="0 1 * * *",  # tous les jours à 01:00
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
        task_id="get_published_tomorrow",
        python_callable=get_published_tomorrow,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="compute_missing",
        python_callable=compute_missing,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="rotate_and_update",
        python_callable=rotate_and_update,
        provide_context=True,
    )

    [t1, t2] >> t3 >> t4
