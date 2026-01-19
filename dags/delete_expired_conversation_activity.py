import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

import psycopg2

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"   # adapte au nom de ta connexion Airflow
RETENTION_DAYS = 5

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def _pg_connect():
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )

def purge_old_conversation_activities(**kwargs):
    with _pg_connect() as connection:
        with connection.cursor() as cur:
            # 1) IDs des conversations à purger
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

            logging.info("🧹 %s ConversationActivity à purger (>%s jours).", len(ids), RETENTION_DAYS)

            # 2) Feedback : on détache (conversation_id = NULL) au lieu de supprimer
            cur.execute(
                """
                UPDATE profil_conversationactivityfeedback
                SET conversation_id = NULL
                WHERE conversation_id = ANY(%s)
                """,
                (ids,),
            )
            logging.info("📝 Feedback détachés (conversation_id=NULL): %s", cur.rowcount)

            # 3) Participants
            cur.execute(
                """
                DELETE FROM profil_participantconversationactivity
                WHERE "conversationActivity_id" = ANY(%s)
                """,
                (ids,),
            )
            logging.info("👥 Participants supprimés: %s", cur.rowcount)

            # 4) Messages
            cur.execute(
                """
                DELETE FROM profil_conversationactivitymessages
                WHERE "conversationActivity_id" = ANY(%s)
                """,
                (ids,),
            )
            logging.info("💬 Messages supprimés: %s", cur.rowcount)

            # 5) Conversations
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

with DAG(
    dag_id="deleted_expired_conversation_activity",
    description="Purge quotidienne des ConversationActivity > 5 jours (messages/participants supprimés, feedback détaché).",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["conversation", "cleanup", "postgres"],
) as dag:
    dag.timezone = PARIS_TZ

    PythonOperator(
        task_id="purge_old_conversation_activities",
        python_callable=purge_old_conversation_activities,
        provide_context=True,
    )
