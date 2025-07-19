from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# Requête SQL complète pour supprimer les conversations expirées et tout ce qui s’y rattache
SQL_DELETE_EXPIRED_CONVERSATIONS = """
-- Supprimer tous les votes liés aux options des sondages
DELETE FROM vote_conversation
WHERE poll_option_id IN (
    SELECT id FROM poll_option_conversation
    WHERE poll_id IN (
        SELECT poll.id
        FROM poll_conversation AS poll
        JOIN conversation_activity_messages AS msg ON poll.conversation_activity_message_id = msg.id
        JOIN conversation_activity AS conv ON msg.conversationactivity_id = conv.id
        WHERE conv.date < NOW()
    )
);

-- Supprimer les options des sondages
DELETE FROM poll_option_conversation
WHERE poll_id IN (
    SELECT poll.id
    FROM poll_conversation AS poll
    JOIN conversation_activity_messages AS msg ON poll.conversation_activity_message_id = msg.id
    JOIN conversation_activity AS conv ON msg.conversationactivity_id = conv.id
    WHERE conv.date < NOW()
);

-- Supprimer les sondages
DELETE FROM poll_conversation
WHERE conversation_activity_message_id IN (
    SELECT msg.id
    FROM conversation_activity_messages AS msg
    JOIN conversation_activity AS conv ON msg.conversationactivity_id = conv.id
    WHERE conv.date < NOW()
);

-- Supprimer les messages liés aux conversations expirées
DELETE FROM conversation_activity_messages
WHERE conversationactivity_id IN (
    SELECT id FROM conversation_activity
    WHERE date < NOW()
);

-- Supprimer les participations
DELETE FROM participant_conversation_activity
WHERE conversationactivity_id IN (
    SELECT id FROM conversation_activity
    WHERE date < NOW()
);

-- Supprimer les conversations elles-mêmes
DELETE FROM conversation_activity
WHERE date < NOW();
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
        postgres_conn_id="my_postgres",  # à adapter si différent
        sql=SQL_DELETE_EXPIRED_CONVERSATIONS,
    )

    delete_expired_conversations_task
