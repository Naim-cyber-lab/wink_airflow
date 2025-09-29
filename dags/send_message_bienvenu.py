# dags/broadcast_from_116.py
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
DB_CONN_ID = "my_postgres"         # Airflow Connection (Postgres)
SENDER_ID = 116                    # <- l'expéditeur demandé
# Contenu du message (configurable via Variables Airflow)
MESSAGE_TEXT = Variable.get(
    """
    👋 Bienvenue sur Nisu !
    Ici, la bienveillance et le respect sont essentiels 💜
    
    ✨ Parrainage : si tu parraines un nouvel utilisateur (en lui faisant envoyer ton pseudo à Nisu Official lors de son inscription), ton compte est boosté 1 mois 🚀 :
    
    Conversations plus visibles
    
    5 conversations au lieu de 3
    
    Accès complet aux messages
    
    Merci d’être parmi nous 🙏 Amuse-toi bien sur Nisu ! 🎉
        """)

# Expo Push
EXPO_API = "https://exp.host/--/api/v2/push/send"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ========= HELPERS =========

def _pg_connect():
    """
    Connexion Postgres via l'Airflow Connection.
    """
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )

def _send_expo(tokens, title, body, data=None):
    """
    Envoie des push Expo par paquets de 100 (filtre tokens valides).
    Inspiré de ton DAG de notif IDF.  :contentReference[oaicite:2]{index=2}
    """
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

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
    }

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
    """
    Récupère la liste des winker.id à qui SENDER_ID n'a jamais écrit (symétrie respectée).
    C’est exactement ta logique, encapsulée ici.
    """
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
      - Assure/Crée la conversation symétrique avec 116 (si absente)
      - Insère le message (winker=116 -> winker2=target)
      - Met à jour lastMessage / lastMessageTime / nbUnseen côté destinataire
      - Récupère le token Expo du destinataire pour notification
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
                    # 1) Trouver conversation existante
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
                        # 2) Créer conversation (on met 116 en winker1 par cohérence)
                        cur.execute(
                            """
                            INSERT INTO profil_chatwinker (winker1_id, winker2_id, "is_chat_group", seen)
                            VALUES (%s, %s, FALSE, FALSE)
                            RETURNING id
                            """,
                            (SENDER_ID, target_id),
                        )
                        chat_id = cur.fetchone()[0]

                    # 3) Insérer le message et récupérer son id
                    cur.execute(
                        """
                        INSERT INTO profil_chatwinkermessagesclass
                            ( "chatWinker_id", "winker_id", "winker2_id",
                              message, "isLiked", seen, is_read, created, modified )
                        VALUES ( %s, %s, %s,
                                 %s, FALSE, FALSE, FALSE, NOW(), NOW() )
                        RETURNING id
                        """,
                        (chat_id, SENDER_ID, target_id, MESSAGE_TEXT),
                    )
                    msg_id = cur.fetchone()[0]

                    # 4) Mettre à jour la conversation : lastMessage, lastMessageTime, nbUnseen côté destinataire
                    cur.execute(
                        """
                        UPDATE profil_chatwinker
                        SET "lastMessage_id" = %s,
                            "lastMessageTime" = NOW(),
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

                    # 5) Récupérer le token Expo du destinataire
                    cur.execute(
                        """SELECT "expoPushToken" FROM profil_winker WHERE id = %s""",
                        (target_id,),
                    )
                    tok = cur.fetchone()
                    if tok and tok[0]:
                        expo_tokens.append(tok[0])

                    inserted_count += 1

            connection.commit()
            logging.info("✉️ Messages insérés/assurés pour %s destinataires.", inserted_count)
        except Exception as e:
            connection.rollback()
            logging.exception("Rollback suite erreur: %s", e)
            raise

    ti.xcom_push(key="expo_tokens", value=expo_tokens)

def push_notifications(**kwargs):
    """
    Envoie la notification Expo à tous les destinataires traités.
    """
    ti = kwargs["ti"]
    tokens = ti.xcom_pull(task_ids="send_messages_and_collect_tokens", key="expo_tokens") or []
    if not tokens:
        logging.info("🔕 Aucun token à notifier.")
        return

    title = "Nouveau message 💬"
    body = "Vous avez reçu un message."
    payload = {
        "type": "chat_message",
        "from": SENDER_ID,
    }

    sent = _send_expo(tokens, title, body, data=payload)
    logging.info("📲 Push envoyé à %s utilisateurs.", sent)

# ========= DAG =========

with DAG(
    dag_id="broadcast_from_116",
    description="Diffuse un message depuis le compte 116 à tous les winkers n'ayant pas encore échangé avec lui, puis envoie des notifications Expo.",
    default_args=default_args,
    schedule_interval=None,     # déclenchement manuel (à ta convenance)
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
