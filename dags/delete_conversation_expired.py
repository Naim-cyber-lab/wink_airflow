from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

SQL_DELETE_EXPIRED_CONVERSATIONS = """
-- Étape 1 : Identifier les conversations expirées
WITH expired_conversations AS (
    SELECT id FROM profil_conversationactivity WHERE date < NOW()
),

-- Étape 2 : Identifier les messages liés
expired_messages AS (
    SELECT id FROM profil_conversationactivitymessages
    WHERE "conversationActivity_id" IN (SELECT id FROM expired_conversations)
),

-- Étape 3 : Identifier les sondages liés
expired_polls AS (
    SELECT id FROM profil_pollconversation
    WHERE "conversation_activity_message_id" IN (SELECT id FROM expired_messages)
),

-- Étape 4 : Identifier les options de sondages liés
expired_poll_options AS (
    SELECT id FROM profil_polloptionconversation
    WHERE poll_id IN (SELECT id FROM expired_polls)
),

-- Supprimer les votes liés aux options
deleted_votes AS (
    DELETE FROM profil_voteconversation
    WHERE poll_option_id IN (SELECT id FROM expired_poll_options)
    RETURNING 1
),

-- Supprimer les options des sondages
deleted_poll_options AS (
    DELETE FROM profil_polloptionconversation
    WHERE id IN (SELECT id FROM expired_poll_options)
    RETURNING 1
),

-- Supprimer les sondages
deleted_polls AS (
    DELETE FROM profil_pollconversation
    WHERE id IN (SELECT id FROM expired_polls)
    RETURNING 1
),

-- Supprimer les messages
deleted_messages AS (
    DELETE FROM profil_conversationactivitymessages
    WHERE id IN (SELECT id FROM expired_messages)
    RETURNING 1
),

-- Supprimer les participations
deleted_participants AS (
    DELETE FROM profil_participantconversationactivity
    WHERE "conversationActivity_id" IN (SELECT id FROM expired_conversations)
    RETURNING 1
),

-- 🔥 Supprimer les préférences liées
deleted_preferences AS (
    DELETE FROM profil_preference
    WHERE "conversation_id" IN (SELECT id FROM expired_conversations)
    RETURNING 1
)

-- Supprimer les conversations
DELETE FROM profil_conversationactivity
WHERE id IN (SELECT id FROM expired_conversations);

"""

with DAG(
    dag_id="delete_expired_conversations",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["cleanup", "conversations"],
) as dag:

    delete_expired_conversations_task = PostgresOperator(
        task_id="delete_expired_conversations_task",
        postgres_conn_id="my_postgres",  # adapte le nom de la connexion si besoin
        sql=SQL_DELETE_EXPIRED_CONVERSATIONS,
    )

    delete_expired_conversations_task
