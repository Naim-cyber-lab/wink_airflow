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
    SELECT id FROM profil_conversation_activity WHERE date < NOW()
),

-- Étape 2 : Identifier les messages liés
expired_messages AS (
    SELECT id FROM profil_conversation_activity_messages
    WHERE conversationactivity_id IN (SELECT id FROM expired_conversations)
),

-- Étape 3 : Identifier les sondages liés
expired_polls AS (
    SELECT id FROM profil_poll_conversation
    WHERE conversation_activity_message_id IN (SELECT id FROM expired_messages)
),

-- Étape 4 : Identifier les options de sondages liés
expired_poll_options AS (
    SELECT id FROM profil_poll_option_conversation
    WHERE poll_id IN (SELECT id FROM expired_polls)
)

-- Supprimer les votes liés aux options
DELETE FROM profil_vote_conversation
WHERE poll_option_id IN (SELECT id FROM expired_poll_options);

-- Supprimer les options des sondages
DELETE FROM profil_poll_option_conversation
WHERE id IN (SELECT id FROM expired_poll_options);

-- Supprimer les sondages
DELETE FROM profil_poll_conversation
WHERE id IN (SELECT id FROM expired_polls);

-- Supprimer les messages
DELETE FROM profil_conversation_activity_messages
WHERE id IN (SELECT id FROM expired_messages);

-- Supprimer les participations
DELETE FROM profil_participant_conversation_activity
WHERE conversationactivity_id IN (SELECT id FROM expired_conversations);

-- Supprimer les conversations
DELETE FROM profil_conversation_activity
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
