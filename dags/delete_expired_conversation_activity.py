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
            # 1) Conversations à purger
            cur.execute(
                """
                SELECT id
                FROM profil_conversationactivity
                WHERE "date" < NOW() - INTERVAL %s
                """,
                (f"{RETENTION_DAYS} days",),
            )
            conv_ids = [r[0] for r in cur.fetchall()]

            if not conv_ids:
                logging.info("✅ Aucun ConversationActivity à purger (>%s jours).", RETENTION_DAYS)
                return

            logging.info("🧹 %s ConversationActivity à purger (>%s jours).", len(conv_ids), RETENTION_DAYS)

            # 2) Feedback : on détache (conversation_id = NULL)
            cur.execute(
                """
                UPDATE profil_conversationactivityfeedback
                SET conversation_id = NULL
                WHERE conversation_id = ANY(%s)
                """,
                (conv_ids,),
            )
            logging.info("📝 Feedback détachés: %s", cur.rowcount)

            # 3) Messages liés aux conversations
            cur.execute(
                """
                SELECT id
                FROM profil_conversationactivitymessages
                WHERE "conversationActivity_id" = ANY(%s)
                """,
                (conv_ids,),
            )
            msg_ids = [r[0] for r in cur.fetchall()]
            logging.info("💬 Messages trouvés à supprimer: %s", len(msg_ids))

            # 3bis) Suppression des sondages liés aux messages
            # PollConversation -> PollOptionConversation -> VoteConversation
            if msg_ids:
                # PollConversation ids
                cur.execute(
                    """
                    SELECT id
                    FROM profil_pollconversation
                    WHERE conversation_activity_message_id = ANY(%s)
                    """,
                    (msg_ids,),
                )
                poll_ids = [r[0] for r in cur.fetchall()]
                logging.info("📊 PollConversation trouvés: %s", len(poll_ids))

                if poll_ids:
                    # PollOptionConversation ids
                    cur.execute(
                        """
                        SELECT id
                        FROM profil_polloptionconversation
                        WHERE poll_id = ANY(%s)
                        """,
                        (poll_ids,),
                    )
                    option_ids = [r[0] for r in cur.fetchall()]
                    logging.info("🧩 PollOptionConversation trouvées: %s", len(option_ids))

                    # VoteConversation (enfant des options)
                    if option_ids:
                        cur.execute(
                            """
                            DELETE FROM profil_voteconversation
                            WHERE poll_option_id = ANY(%s)
                            """,
                            (option_ids,),
                        )
                        logging.info("🗳️ VoteConversation supprimés: %s", cur.rowcount)

                        # Options
                        cur.execute(
                            """
                            DELETE FROM profil_polloptionconversation
                            WHERE id = ANY(%s)
                            """,
                            (option_ids,),
                        )
                        logging.info("🧩 PollOptionConversation supprimées: %s", cur.rowcount)
                    else:
                        logging.info("ℹ️ Aucune PollOptionConversation à supprimer.")

                    # Polls
                    cur.execute(
                        """
                        DELETE FROM profil_pollconversation
                        WHERE id = ANY(%s)
                        """,
                        (poll_ids,),
                    )
                    logging.info("📊 PollConversation supprimés: %s", cur.rowcount)
                else:
                    logging.info("ℹ️ Aucun PollConversation lié à ces messages.")
            else:
                logging.info("ℹ️ Aucun message => aucun sondage à supprimer.")

            # 4) Réactions sur ces messages
            if msg_ids:
                cur.execute(
                    """
                    DELETE FROM profil_conversationactivitymessagereaction
                    WHERE message_id = ANY(%s)
                    """,
                    (msg_ids,),
                )
                logging.info("❤️ Réactions supprimées: %s", cur.rowcount)

                # 5) Suppression des messages
                cur.execute(
                    """
                    DELETE FROM profil_conversationactivitymessages
                    WHERE id = ANY(%s)
                    """,
                    (msg_ids,),
                )
                logging.info("💬 Messages supprimés: %s", cur.rowcount)
            else:
                logging.info("ℹ️ Aucun message à supprimer pour ces conversations.")

            # 6) Participants liés
            cur.execute(
                """
                DELETE FROM profil_participantconversationactivity
                WHERE "conversationActivity_id" = ANY(%s)
                """,
                (conv_ids,),
            )
            logging.info("👥 Participants supprimés: %s", cur.rowcount)

            # 6bis) Seen / last seen par conversation (⚠️ colonne camelCase => guillemets obligatoires)
            cur.execute(
                """
                DELETE FROM seen_winker_activity
                WHERE "conversationActivity_id" = ANY(%s)
                """,
                (conv_ids,),
            )
            logging.info("👁️ seen_winker_activity supprimés: %s", cur.rowcount)

            # 6ter) ConfirmParticipationModal liés aux conversations
            cur.execute(
                """
                DELETE FROM profil_confirmparticipationmodal
                WHERE conversation_activity_id = ANY(%s)
                """,
                (conv_ids,),
            )
            logging.info("✅ profil_confirmparticipationmodal supprimés: %s", cur.rowcount)

            # 7) Suppression des conversations
            cur.execute(
                """
                DELETE FROM profil_conversationactivity
                WHERE id = ANY(%s)
                """,
                (conv_ids,),
            )
            logging.info("💥 Conversations supprimées: %s", cur.rowcount)

            connection.commit()
            logging.info("✅ Purge terminée.")


with DAG(
    dag_id="purge_old_conversation_activities",
    description="Purge quotidienne des ConversationActivity > 5 jours (feedback detach, polls/votes/options, reactions/messages/participants/seen/confirmParticipation deleted).",
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
