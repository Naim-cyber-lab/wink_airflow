from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

SQL_DELETE_EXPIRED_CONVERSATIONS = """
WITH expired AS (
    SELECT
        id,
        COALESCE(nb_conversations_scheduled, 0)         AS n_sched,
        COALESCE(interval_days_scheduled, 0)            AS d_gap,
        COALESCE(datePublication, CURRENT_DATE)::date   AS dp
    FROM profil_conversationactivity
    WHERE date < NOW()
),

/* ---------- Cas A : à replanifier ---------- */
to_reschedule AS (
    SELECT id FROM expired WHERE n_sched > 1
),
do_reschedule AS (
    UPDATE profil_conversationactivity c
    SET
        datePublication = (COALESCE(c.datePublication, CURRENT_DATE)::date
                           + (INTERVAL '1 day' * COALESCE(c.interval_days_scheduled, 0)))::date,
        nb_conversations_scheduled = GREATEST(COALESCE(c.nb_conversations_scheduled, 0) - 1, 0)
    FROM to_reschedule r
    WHERE c.id = r.id
    RETURNING c.id
),

/* ---------- Cas B : à supprimer (stories conservées) ---------- */
to_delete AS (
    SELECT id FROM expired
    EXCEPT
    SELECT id FROM to_reschedule
),

/* Enfants liés aux conversations à supprimer */
expired_messages AS (
    SELECT id FROM profil_conversationactivitymessages
    WHERE "conversationActivity_id" IN (SELECT id FROM to_delete)
),
expired_polls AS (
    SELECT id FROM profil_pollconversation
    WHERE "conversation_activity_message_id" IN (SELECT id FROM expired_messages)
),
expired_poll_options AS (
    SELECT id FROM profil_polloptionconversation
    WHERE poll_id IN (SELECT id FROM expired_polls)
),

/* Purge ordonnée des dépendances */
deleted_votes AS (
    DELETE FROM profil_voteconversation
    WHERE poll_option_id IN (SELECT id FROM expired_poll_options)
    RETURNING 1
),
deleted_poll_options AS (
    DELETE FROM profil_polloptionconversation
    WHERE id IN (SELECT id FROM expired_poll_options)
    RETURNING 1
),
deleted_polls AS (
    DELETE FROM profil_pollconversation
    WHERE id IN (SELECT id FROM expired_polls)
    RETURNING 1
),
deleted_messages AS (
    DELETE FROM profil_conversationactivitymessages
    WHERE id IN (SELECT id FROM expired_messages)
    RETURNING 1
),
deleted_participants AS (
    DELETE FROM profil_participantconversationactivity
    WHERE "conversationActivity_id" IN (SELECT id FROM to_delete)
    RETURNING 1
),
deleted_preferences AS (
    DELETE FROM profil_preference
    WHERE "conversation_id" IN (SELECT id FROM to_delete)
    RETURNING 1
),
deleted_notifications AS (
    DELETE FROM profil_notification
    WHERE "conversation_id" IN (SELECT id FROM to_delete)
    RETURNING 1
),

/* Très important : détacher les stories avant le DELETE (on garde les stories) */
nulled_stories AS (
    UPDATE stories
    SET conversation_id = NULL
    WHERE conversation_id IN (SELECT id FROM to_delete)
    RETURNING 1
)

/* Enfin : supprimer les conversations à supprimer */
DELETE FROM profil_conversationactivity
WHERE id IN (SELECT id FROM to_delete);
"""

PARIS_TZ = ZoneInfo("Europe/Paris")


with DAG(
    dag_id="delete_expired_conversations",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["cleanup", "conversations"]
) as dag:
    dag.timezone = PARIS_TZ

    delete_expired_conversations_task = PostgresOperator(
        task_id="delete_expired_conversations_task",
        postgres_conn_id="my_postgres",  # adapte le nom de la connexion si besoin
        sql=SQL_DELETE_EXPIRED_CONVERSATIONS,
    )

    delete_expired_conversations_task
