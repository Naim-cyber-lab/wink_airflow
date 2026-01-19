import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

import psycopg2

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ================== CONFIG ==================

PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"  # adapte au nom de ta connexion Postgres
RETENTION_DAYS = 5

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

# ================== CORE TASK ==================

def purge_old_conversation_activities(**kwargs):
    """
    Supprime les ConversationActivity plus vieilles que RETENTION_DAYS.
    - On supprime d'abord les dépendances connues (participants)
      pour éviter les erreurs de FK si pas de cascade.
    """
    with _pg_connect() as connection:
        with connection.cursor() as cur:
            # 1) Récupère les ids à supprimer (utile pour logs + deletes liés)
            cur.execute(
                """
                SELECT id
                FROM profil_conversationactivity
                WHERE "date" < NOW() - INTERVAL %s
                """,
                (f"{RETENTION_DAYS} days",),
            )
            ids = [r[0] for r in cur.fetchall()]

            if not ids:
                logging.info("✅ Aucun ConversationActivity à supprimer (>%s jours).", RETENTION_DAYS)
                return

            logging.info("🧹 %s ConversationActivity à supprimer (>%s jours).", len(ids), RETENTION_DAYS)

            # 2) Supprime les participants liés (FK classique vers conversationActivity_id)
            cur.execute(
                """
                DELETE FROM profil_participantconversationactivity
                WHERE "conversationActivity_id" = ANY(%s)
                """,
                (ids,),
            )
            logging.info("👥 Participants supprimés: %s", cur.rowcount)

            # 3) Supprime les conversations
            cur.execute(
                """
                DELETE FROM profil_conversationactivity
                WHERE id = ANY(%s)
                """,
                (ids,),
            )
            logging.info("💥 Conversations supprimées: %s", cur.rowcount)

            connection.commit()
            logging.info("✅ Purge terminée.")

# ================== DAG ==================

with DAG(
    dag_id="purge_old_conversation_activities",
    description="Supprime chaque jour les ConversationActivity dont la date est plus vieille que 5 jours.",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["conversation", "cleanup", "postgres"],
) as dag:
    dag.timezone = PARIS_TZ

    purge_task = PythonOperator(
        task_id="purge_old_conversation_activities",
        python_callable=purge_old_conversation_activities,
        provide_context=True,
    )
