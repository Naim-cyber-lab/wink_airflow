from zoneinfo import ZoneInfo
from datetime import datetime
import json
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

SQL_DELETE_EXPIRED_CONVERSATIONS = """
WITH expired AS (
    SELECT
        id,
        COALESCE(nb_conversations_scheduled, 0)  AS n_sched,
        COALESCE(interval_days_scheduled, 0)     AS d_gap,
        COALESCE("datePublication", CURRENT_DATE)  AS dp
    FROM profil_conversationactivity
    WHERE "date" < NOW()
),

/* ---------- Cas A : à replanifier ---------- */
to_reschedule AS (
    SELECT id FROM expired WHERE n_sched > 1
),
do_reschedule AS (
    UPDATE profil_conversationactivity c
    SET
        "datePublication" = (COALESCE(c."datePublication", CURRENT_DATE)::date
                           + (INTERVAL '1 day' * COALESCE(c.interval_days_scheduled, 0)))::date,
        nb_conversations_scheduled = GREATEST(COALESCE(c.nb_conversations_scheduled, 0) - 1, 0)
    FROM to_reschedule r
    WHERE c.id = r.id
    RETURNING c.id
),
rescheduled_count AS (
    SELECT COUNT(*)::int AS rescheduled FROM do_reschedule
),

/* ---------- Cas B : à supprimer (stories conservées) ---------- */
to_delete AS (
    SELECT id FROM expired
    EXCEPT
    SELECT id FROM to_reschedule
),

/* ----------- MESSAGES CHAT liés aux conversations à supprimer ----------- */
expired_chat_msgs AS (
    SELECT id
    FROM profil_chatwinkermessagesclass
    WHERE conversation_id IN (SELECT id FROM to_delete)
),

/* 🔥 NEW : on nettoie les références lastMessage_id dans profil_chatwinker */
nulled_chatwinker_last_msg AS (
    UPDATE profil_chatwinker cw
    SET "lastMessage_id" = NULL
    WHERE "lastMessage_id" IN (SELECT id FROM expired_chat_msgs)
    RETURNING 1
),

deleted_chat_msgs AS (
    DELETE FROM profil_chatwinkermessagesclass
    WHERE id IN (SELECT id FROM expired_chat_msgs)
    RETURNING 1
),

/* Enfants liés aux conversations à supprimer (messages "activity") */
expired_messages AS (
    SELECT id FROM profil_conversationactivitymessages
    WHERE "conversationActivity_id" IN (SELECT id FROM to_delete)
),

/* 🔥 Réactions liées aux messages expirés */
expired_reactions AS (
    SELECT id
    FROM profil_conversationactivitymessagereaction
    WHERE message_id IN (SELECT id FROM expired_messages)
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

/* 🔥 Suppression des réactions AVANT les messages */
deleted_reactions AS (
    DELETE FROM profil_conversationactivitymessagereaction
    WHERE message_id IN (SELECT id FROM expired_messages)
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
nulled_last_message AS (
    UPDATE profil_conversationactivity ca
    SET "lastMessage_id" = NULL
    WHERE "lastMessage_id" IN (SELECT id FROM expired_messages)
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

/* Détacher les stories (on les garde) */
nulled_stories AS (
    UPDATE stories
    SET conversation_id = NULL
    WHERE conversation_id IN (SELECT id FROM to_delete)
    RETURNING 1
),
deleted_seen_flags AS (
    DELETE FROM seen_winker_activity
    WHERE "conversationActivity_id" IN (SELECT id FROM to_delete)
    RETURNING 1
),

/* Supprimer les conversations à supprimer */
deleted_conversations AS (
   DELETE FROM profil_conversationactivity
   WHERE id IN (SELECT id FROM to_delete)
   RETURNING 1
),

/* Compteurs pour les logs */
counts AS (
    SELECT
        (SELECT rescheduled FROM rescheduled_count)              AS rescheduled,
        (SELECT COUNT(*)::int FROM to_delete)                    AS to_delete,
        (SELECT COUNT(*)::int FROM nulled_stories)               AS stories_detached,
        (SELECT COUNT(*)::int FROM deleted_messages)             AS messages_deleted,
        (SELECT COUNT(*)::int FROM deleted_polls)                AS polls_deleted,
        (SELECT COUNT(*)::int FROM deleted_poll_options)         AS poll_options_deleted,
        (SELECT COUNT(*)::int FROM deleted_votes)                AS votes_deleted,
        (SELECT COUNT(*)::int FROM deleted_reactions)            AS reactions_deleted,
        (SELECT COUNT(*)::int FROM deleted_participants)         AS participants_deleted,
        (SELECT COUNT(*)::int FROM deleted_preferences)          AS preferences_deleted,
        (SELECT COUNT(*)::int FROM deleted_notifications)        AS notifications_deleted,
        (SELECT COUNT(*)::int FROM deleted_seen_flags)           AS seen_flags_deleted,
        (SELECT COUNT(*)::int FROM deleted_chat_msgs)            AS chat_msgs_deleted,
        (SELECT COUNT(*)::int FROM deleted_conversations)        AS conversations_deleted
)
SELECT * FROM counts;

"""

PARIS_TZ = ZoneInfo("Europe/Paris")


def log_cleanup_counts(ti, **_):
    """
    Récupère la ligne renvoyée par le PostgresOperator via XCom
    et l'affiche joliment dans les logs Airflow.
    """
    records = ti.xcom_pull(task_ids="delete_expired_conversations_task")
    # PostgresOperator renvoie généralement une liste de tuples
    if not records:
        logging.info("Aucun résultat renvoyé par la requête (records est vide).")
        return

    row = records[0]
    # L'ordre des colonnes doit correspondre au SELECT final
    keys = [
        "rescheduled",
        "to_delete",
        "stories_detached",
        "messages_deleted",
        "polls_deleted",
        "poll_options_deleted",
        "votes_deleted",
        "reactions_deleted",
        "participants_deleted",
        "preferences_deleted",
        "notifications_deleted",
        "seen_flags_deleted",
        "chat_msgs_deleted",
        "conversations_deleted",
    ]
    # Sécurise le mapping en cas de driver différent
    payload = {keys[i]: row[i] for i in range(min(len(keys), len(row)))}

    logging.info("=== DeleteExpiredConversations - Counters ===")
    logging.info(json.dumps(payload, ensure_ascii=False, indent=2))
    logging.info("===========================================")


with DAG(
    dag_id="delete_expired_conversations",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["cleanup", "conversations"],
) as dag:
    dag.timezone = PARIS_TZ

    delete_expired_conversations_task = PostgresOperator(
        task_id="delete_expired_conversations_task",
        postgres_conn_id="my_postgres",
        sql=SQL_DELETE_EXPIRED_CONVERSATIONS,
        do_xcom_push=True,  # <— indispensable pour récupérer les compteurs
    )

    log_counts_task = PythonOperator(
        task_id="log_cleanup_counts",
        python_callable=log_cleanup_counts,
    )

    delete_expired_conversations_task >> log_counts_task
