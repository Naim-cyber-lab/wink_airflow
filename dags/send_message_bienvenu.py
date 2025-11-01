# dags/send_message_bienvenu.py
import os
import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

import psycopg2
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ========= CONFIG =========
PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"          # Airflow Connection (Postgres)
SENDER_ID = 116                     # l'expéditeur

# Texte du message (modifiable via Variables Airflow)
MESSAGE_TEXT = Variable.get(
    "BROADCAST_MESSAGE_116",
    default_var=(
        """
        👋 Bienvenue sur Nisu,
        Ici, on se retrouve pour partager des sorties, des rires et de belles rencontres 🌟,
        Notre mot d’ordre : bienveillance, respect et bonne humeur 💬💛,
        Si tu as la moindre question, tu peux nous écrire directement ici.
        On est ravis de t’accueillir dans la communauté et à très vite pour ta première sortie ! 🚀✨
        """
    )
)

# Expo Push
EXPO_API = "https://exp.host/--/api/v2/push/send"
EXPO_ACCESS_TOKEN = Variable.get("EXPO_ACCESS_TOKEN", default_var=os.getenv("EXPO_ACCESS_TOKEN", ""))

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ========= HELPERS =========

def _pg_connect():
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )

def _send_expo(tokens, title, body, data=None):
    messages = [
        {
            "to": t,
            "title": title,
            "body": body,
            "sound": "default",
            "data": data or {},
            "priority": "high",
        }
        for t in tokens
        if t and isinstance(t, str) and t.startswith("ExponentPushToken")
    ]
    if not messages:
        logging.info("🔕 Aucun token Expo valide.")
        return 0

    headers = {"accept": "application/json", "content-type": "application/json"}
    if EXPO_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {EXPO_ACCESS_TOKEN}"

    sent = 0
    for i in range(0, len(messages), 100):
        chunk = messages[i : i + 100]
        try:
            r = requests.post(EXPO_API, json=chunk, headers=headers, timeout=15)
            r.raise_for_status()
            res = r.json()
            try:
                data_items = res.get("data") or []
                errors = [d for d in data_items if isinstance(d, dict) and d.get("status") != "ok"]
                if errors:
                    logging.warning("Expo push errors: %s", errors)
            except Exception:
                pass
            sent += len(chunk)
        except Exception as e:
            logging.exception("Expo push failed on chunk: %s", e)
    return sent

# ========= TASKS =========

def get_targets(**kwargs):
    ti = kwargs["ti"]
    with _pg_connect() as connection, connection.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM profil_winker
            WHERE id NOT IN (
                SELECT DISTINCT
                    CASE
                        WHEN winker1_id = %s THEN winker2_id
                        ELSE winker1_id
                    END AS other_winker_id
                FROM profil_chatwinker
                WHERE %s IN (winker1_id, winker2_id)
                  AND winker1_id <> winker2_id
            )
              AND id <> %s
            """,
            (SENDER_ID, SENDER_ID, SENDER_ID),
        )
        targets = [row[0] for row in cur.fetchall()]
    logging.info("🎯 %s cibles trouvées (à contacter depuis %s).", len(targets), SENDER_ID)
    ti.xcom_push(key="targets", value=targets)

def send_messages_and_collect_tokens(**kwargs):
    """
    Pour chaque cible :
      1) Crée/assure la conversation 116 <-> target (renseigne created/modified/lastMessageTime)
      2) Insère le message (116 -> target) en renseignant aussi is_removed / isSaved
      3) Met à jour lastMessage/lastMessageTime/compteurs côté destinataire
      4) Récupère le token Expo du destinataire
    """
    ti = kwargs["ti"]
    targets = ti.xcom_pull(task_ids="get_targets", key="targets") or []
    if not targets:
        logging.info("Aucune cible à traiter.")
        ti.xcom_push(key="expo_tokens", value=[])
        return

    expo_tokens = []
    inserted_count = 0

    with _pg_connect() as connection:
        connection.autocommit = False
        try:
            with connection.cursor() as cur:
                for target_id in targets:
                    # 1) Conversation existante ?
                    cur.execute(
                        """
                        SELECT id
                        FROM profil_chatwinker
                        WHERE (winker1_id = %s AND winker2_id = %s)
                           OR (winker1_id = %s AND winker2_id = %s)
                        LIMIT 1
                        """,
                        (SENDER_ID, target_id, target_id, SENDER_ID),
                    )
                    row = cur.fetchone()

                    if row:
                        chat_id = row[0]
                    else:
                        # 2) Créer la conversation (TimeStampedModel => created/modified obligatoires)
                        cur.execute(
                            """
                            INSERT INTO profil_chatwinker
                                (created, modified,
                                 winker1_id, winker2_id,
                                 is_chat_group, seen,
                                 "nbUnseenWinker1", "nbUnseenWinker2",
                                 "lastMessageTime",
                                 "dataNbNotif", "connectedUser", "allUsers")
                            VALUES
                                (NOW(), NOW(),
                                 %s, %s,
                                 FALSE, FALSE,
                                 0, 0,
                                 NOW(),
                                 '{}', '[]', '')
                            RETURNING id
                            """,
                            (SENDER_ID, target_id),
                        )
                        chat_id = cur.fetchone()[0]

                    # 3) Insérer le message (⚠️ SoftDeletableModel => is_removed NOT NULL)
                    cur.execute(
                        """
                        INSERT INTO profil_chatwinkermessagesclass
                            (created, modified, is_removed,
                             "chatWinker_id", "winker_id", "winker2_id",
                             message, "isLiked", seen, is_read, "isSaved")
                        VALUES
                            (NOW(), NOW(), FALSE,
                             %s, %s, %s,
                             %s, FALSE, FALSE, FALSE, FALSE)
                        RETURNING id
                        """,
                        (chat_id, SENDER_ID, target_id, MESSAGE_TEXT),
                    )
                    msg_id = cur.fetchone()[0]

                    # 4) Maj conversation (lastMessage/Time + unseen côté destinataire + modified)
                    cur.execute(
                        """
                        UPDATE profil_chatwinker
                        SET "lastMessage_id" = %s,
                            "lastMessageTime" = NOW(),
                            modified = NOW(),
                            "nbUnseenWinker1" = CASE
                                WHEN winker1_id = %s THEN "nbUnseenWinker1" + 1
                                ELSE "nbUnseenWinker1"
                            END,
                            "nbUnseenWinker2" = CASE
                                WHEN winker2_id = %s THEN "nbUnseenWinker2" + 1
                                ELSE "nbUnseenWinker2"
                            END
                        WHERE id = %s
                        """,
                        (msg_id, target_id, target_id, chat_id),
                    )

                    # 5) Token Expo du destinataire
                    cur.execute(
                        'SELECT "expoPushToken" FROM profil_winker WHERE id = %s',
                        (target_id,),
                    )
                    tok = cur.fetchone()
                    if tok and tok[0]:
                        expo_tokens.append(tok[0])

                    inserted_count += 1

            connection.commit()
            logging.info("✉️ Messages créés/envoyés pour %s destinataires.", inserted_count)
        except Exception as e:
            connection.rollback()
            logging.exception("Rollback suite erreur: %s", e)
            raise

    ti.xcom_push(key="expo_tokens", value=expo_tokens)

def push_notifications(**kwargs):
    ti = kwargs["ti"]
    tokens = ti.xcom_pull(task_ids="send_messages_and_collect_tokens", key="expo_tokens") or []
    if not tokens:
        logging.info("🔕 Aucun token à notifier.")
        return

    title = "Nouveau message 💬"
    body = "Vous avez reçu un message."
    payload = {"type": "chat_message", "from": SENDER_ID}

    sent = _send_expo(tokens, title, body, data=payload)
    logging.info("📲 Push envoyé à %s utilisateurs.", sent)

# ========= DAG =========

with DAG(
    dag_id="broadcast_from_116",
    description="Crée la conversation si besoin, envoie un message depuis 116 aux cibles, puis push Expo.",
    default_args=default_args,
    schedule_interval=None,        # déclenchement manuel
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["chat", "broadcast", "expo", "116"],
) as dag:
    dag.timezone = PARIS_TZ

    t1 = PythonOperator(
        task_id="get_targets",
        python_callable=get_targets,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="send_messages_and_collect_tokens",
        python_callable=send_messages_and_collect_tokens,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="push_notifications",
        python_callable=push_notifications,
        provide_context=True,
    )

    t1 >> t2 >> t3
