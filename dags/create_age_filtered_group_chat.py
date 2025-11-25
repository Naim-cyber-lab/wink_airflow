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
DB_CONN_ID = "my_postgres"

EXPO_API = "https://exp.host/--/api/v2/push/send"
EXPO_ACCESS_TOKEN = Variable.get(
    "EXPO_ACCESS_TOKEN", default_var=os.getenv("EXPO_ACCESS_TOKEN", "")
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ========= HELPERS =========

def _pg_connect():
    """
    Connexion Postgres à partir de la Connection Airflow `my_postgres`.
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
    Envoi de notifications Expo en chunk de 100 (même logique que ton DAG de broadcast).
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

    headers = {"accept": "application/json", "content-type": "application/json"}
    if EXPO_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {EXPO_ACCESS_TOKEN}"

    sent = 0
    for i in range(0, len(messages), 100):
        chunk = messages[i: i + 100]
        try:
            r = requests.post(EXPO_API, json=chunk, headers=headers, timeout=15)
            r.raise_for_status()
            res = r.json()
            try:
                data_items = res.get("data") or []
                errors = [d for d in data_items
                          if isinstance(d, dict) and d.get("status") != "ok"]
                if errors:
                    logging.warning("Expo push errors: %s", errors)
            except Exception:
                pass
            sent += len(chunk)
        except Exception as e:
            logging.exception("Expo push failed on chunk: %s", e)
    return sent


# ========= TASKS =========

def get_winkers_for_group(**kwargs):
    """
    Récupère tous les winkers dont l'âge est compris entre min_age et max_age
    (exclus le créateur) et pousse la liste d'IDs via XCom.
    """
    ti = kwargs["ti"]
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    try:
        min_age = int(conf.get("min_age"))
        max_age = int(conf.get("max_age"))
        creator_id = int(conf.get("creator_id"))
    except Exception as e:
        raise ValueError(
            "Les paramètres min_age, max_age et creator_id sont obligatoires "
            "et doivent être des entiers."
        ) from e

    if min_age > max_age:
        raise ValueError("min_age ne peut pas être supérieur à max_age.")

    with _pg_connect() as connection, connection.cursor() as cur:
        # ⚠️ ADAPTER selon ton schéma réel :
        # - Si tu as une colonne 'age' (int) dans profil_winker, la requête ci-dessous est OK.
        # - Si tu as 'date_of_birth', calcule l'âge avec AGE(NOW(), date_of_birth).
        cur.execute(
            """
            SELECT id
            FROM profil_winker
            WHERE age BETWEEN %s AND %s
              AND id <> %s
            """,
            (min_age, max_age, creator_id),
        )
        rows = cur.fetchall()

    winker_ids = [r[0] for r in rows]
    logging.info(
        "🎯 %s winkers trouvés pour le groupe (age %s–%s, creator_id=%s).",
        len(winker_ids), min_age, max_age, creator_id
    )

    ti.xcom_push(key="winker_ids", value=winker_ids)


def create_group_conversation(**kwargs):
    """
    Crée la conversation de groupe dans profil_chatwinker avec :
      - is_chat_group = TRUE
      - creatorWinkerGroupChat_id = creator_id
      - winker1_id = creator_id, winker2_id = NULL
      - listIdGroupWinker = liste des ids (créateur + participants) séparés par des virgules.
    Pousse l'ID de la conversation et la liste finale des participants via XCom.
    """
    ti = kwargs["ti"]
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    try:
        creator_id = int(conf.get("creator_id"))
    except Exception as e:
        raise ValueError("Le paramètre creator_id est obligatoire.") from e

    group_title = conf.get("group_title") or "Conversation de groupe"
    winker_ids = ti.xcom_pull(task_ids="get_winkers_for_group", key="winker_ids") or []

    # Participants finaux = créateur + tous ceux dans la tranche d'âge
    participant_ids = sorted({creator_id, *winker_ids})
    list_id_str = ",".join(str(i) for i in participant_ids)

    with _pg_connect() as connection:
        connection.autocommit = False
        try:
            with connection.cursor() as cur:
                # Création de la conversation de groupe
                cur.execute(
                    """
                    INSERT INTO profil_chatwinker
                        (created, modified,
                         winker1_id, winker2_id,
                         is_chat_group,
                         "creatorWinkerGroupChat_id",
                         "listIdGroupWinker",
                         "titreGroupe",
                         "lastMessageTime",
                         "nbUnseenWinker1", "nbUnseenWinker2",
                         "dataNbNotif", "connectedUser", "allUsers",
                         seen)
                    VALUES
                        (NOW(), NOW(),
                         %s, NULL,
                         TRUE,
                         %s,
                         %s,
                         %s,
                         NOW(),
                         0, 0,
                         '{}', '[]', '',
                         FALSE)
                    RETURNING id
                    """,
                    (creator_id, creator_id, list_id_str, group_title),
                )
                chat_id = cur.fetchone()[0]

                # (Optionnel) Si tu as un modèle WinkersChatGroup, c'est ici que tu pourrais
                # insérer les lignes de membership (winker_id, chatWinker_id, ...).

            connection.commit()
            logging.info(
                "💬 Conversation de groupe créée (id=%s, titre='%s', nb_participants=%s).",
                chat_id, group_title, len(participant_ids),
            )
        except Exception as e:
            connection.rollback()
            logging.exception("Rollback suite erreur dans create_group_conversation: %s", e)
            raise

    ti.xcom_push(key="chat_id", value=chat_id)
    ti.xcom_push(key="participant_ids", value=participant_ids)


def notify_group_participants(**kwargs):
    """
    Récupère les tokens Expo de tous les participants (créateur inclus)
    et envoie une notification pour indiquer qu'ils ont été ajoutés au groupe.
    """
    ti = kwargs["ti"]
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    group_title = conf.get("group_title") or "Conversation de groupe"
    chat_id = ti.xcom_pull(task_ids="create_group_conversation", key="chat_id")
    participant_ids = ti.xcom_pull(task_ids="create_group_conversation", key="participant_ids") or []

    if not chat_id or not participant_ids:
        logging.info("Rien à notifier : chat_id ou participant_ids manquants.")
        return

    with _pg_connect() as connection, connection.cursor() as cur:
        cur.execute(
            """
            SELECT "expoPushToken"
            FROM profil_winker
            WHERE id = ANY(%s)
            """,
            (participant_ids,),
        )
        rows = cur.fetchall()

    tokens = [r[0] for r in rows if r and r[0]]
    if not tokens:
        logging.info("🔕 Aucun token Expo pour les participants à notifier.")
        return

    title = "Nouveau groupe 💬"
    body = f"Tu as été ajouté(e) au groupe : {group_title}"
    payload = {
        "type": "group_chat_added",
        "chat_id": chat_id,
        "group_title": group_title,
    }

    sent = _send_expo(tokens, title, body, data=payload)
    logging.info("📲 Notification envoyée à %s participants pour le groupe %s.", sent, chat_id)


# ========= DAG =========

with DAG(
    dag_id="create_age_filtered_group_chat",
    description=(
        "Crée une conversation de groupe en filtrant les winkers par âge, "
        "puis envoie une notification Expo à tous les participants."
    ),
    default_args=default_args,
    schedule_interval=None,  # déclenchement manuel
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["chat", "group", "expo", "age_filter"],
) as dag:
    dag.timezone = PARIS_TZ

    t1 = PythonOperator(
        task_id="get_winkers_for_group",
        python_callable=get_winkers_for_group,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="create_group_conversation",
        python_callable=create_group_conversation,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="notify_group_participants",
        python_callable=notify_group_participants,
        provide_context=True,
    )

    t1 >> t2 >> t3
