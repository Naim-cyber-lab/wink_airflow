# dags/send_message_bienvenu.py
import os
import logging
import math
from datetime import timedelta
from zoneinfo import ZoneInfo

import psycopg2
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ========= CONFIG =========
PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"  # Airflow Connection (Postgres)
SENDER_ID = 116  # l'expéditeur

# Coordonnées Paris (centre)
PARIS_LAT = 48.8566
PARIS_LON = 2.3522
PARIS_RADIUS_KM = 35.0

# Texte du message de bienvenue (modifiable via Variables Airflow)
WELCOME_MESSAGE_TEXT = Variable.get(
    "BROADCAST_MESSAGE_116",
    default_var=(
        """
        👋 Bienvenue sur Nisu,
        Ici, on se retrouve pour partager des sorties, des rires et de belles rencontres 🌟,
        Notre mot d’ordre : bienveillance, respect et bonne humeur 💬💛,
        Si tu as la moindre question, tu peux nous écrire directement ici.
        On est ravis de t’accueillir dans la communauté et à très vite pour ta première sortie ! 🚀✨
        """
    ),
)

WELCOME_MESSAGE_TEXT_SHORT = Variable.get(
    "BROADCAST_MESSAGE_116",
    default_var=(
        """
        👋 Bienvenue sur Nisu,
        Ici, on se retrouve pour partager des sorties, des rires et de belles rencontres 🌟,
        """
    ),
)

# Texte ajouté uniquement pour les utilisateurs proches de Paris (<= 35km)
PARIS_RECO_INTRO_TEXT = Variable.get(
    "BROADCAST_PARIS_RECO_INTRO_116",
    default_var=(
        "🎲🍻 Petite reco Nisu (Paris) : on a 2 events partenaires avec Nisu ! "
        "Ce sont des bars à jeux sur Paris, parfaits pour briser la glace et aider les gens à se connaître. "
        "Je te les envoie juste après 👇"
    ),
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


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    """
    Distance grand cercle en km.
    """
    # Rayon moyen Terre en km
    R = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c


def _get_optional_conversation_activity_ids(kwargs):
    """
    Inputs (facultatifs) : conversation_activity_id_1 / conversation_activity_id_2
    via params (priorité) ou dag_run.conf.
    """
    params = kwargs.get("params") or {}
    conf = (kwargs.get("dag_run").conf or {}) if kwargs.get("dag_run") else {}

    ca1 = params.get("conversation_activity_id_1", conf.get("conversation_activity_id_1"))
    ca2 = params.get("conversation_activity_id_2", conf.get("conversation_activity_id_2"))

    # Les champs sont optionnels globalement : on cast seulement si présents.
    ca1 = int(ca1) if ca1 not in (None, "", 0, "0") else None
    ca2 = int(ca2) if ca2 not in (None, "", 0, "0") else None
    return ca1, ca2


def _assert_conversation_activities_exist(cur, ca1: int, ca2: int):
    """
    Sécurise : évite d’insérer un FK conversation_id qui n’existe pas.
    """
    ids = [x for x in (ca1, ca2) if x is not None]
    if not ids:
        return

    # Table très probablement "profil_conversationactivity" (à adapter si ton nom réel diffère)
    cur.execute(
        """
        SELECT id
        FROM profil_conversationactivity
        WHERE id = ANY(%s)
        """,
        (ids,),
    )
    found = {r[0] for r in cur.fetchall()}
    missing = [x for x in ids if x not in found]
    if missing:
        raise ValueError(f"ConversationActivity id(s) not found in profil_conversationactivity: {missing}")


def _upsert_chat(cur, target_id: int) -> int:
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
        return row[0]

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
    return cur.fetchone()[0]


def _insert_text_message(cur, chat_id: int, target_id: int, text: str) -> int:
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
        (chat_id, SENDER_ID, target_id, text),
    )
    return cur.fetchone()[0]


def _insert_conversation_activity_message(cur, chat_id: int, target_id: int, conversation_activity_id: int) -> int:
    """
    Message qui “pointe” vers une ConversationActivity (FK conversation_id).
    On laisse message NULL (ou tu peux mettre un petit texte si tu veux).
    """
    cur.execute(
        """
        INSERT INTO profil_chatwinkermessagesclass
            (created, modified, is_removed,
             "chatWinker_id", "winker_id", "winker2_id",
             message, conversation_id,
             "isLiked", seen, is_read, "isSaved")
        VALUES
            (NOW(), NOW(), FALSE,
             %s, %s, %s,
             %s, %s,
             FALSE, FALSE, FALSE, FALSE)
        RETURNING id
        """,
        (chat_id, SENDER_ID, target_id, None, conversation_activity_id),
    )
    return cur.fetchone()[0]


def _update_chat_last_and_unseen(cur, chat_id: int, target_id: int, last_msg_id: int, inc: int):
    cur.execute(
        """
        UPDATE profil_chatwinker
        SET "lastMessage_id" = %s,
            "lastMessageTime" = NOW(),
            modified = NOW(),
            "nbUnseenWinker1" = CASE
                WHEN winker1_id = %s THEN "nbUnseenWinker1" + %s
                ELSE "nbUnseenWinker1"
            END,
            "nbUnseenWinker2" = CASE
                WHEN winker2_id = %s THEN "nbUnseenWinker2" + %s
                ELSE "nbUnseenWinker2"
            END
        WHERE id = %s
        """,
        (last_msg_id, target_id, inc, target_id, inc, chat_id),
    )


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
    Règle demandée :
      - Si user est à > 35 km de Paris (lat/lon), on envoie UNIQUEMENT le message de bienvenue.
      - Sinon (<= 35 km), on envoie :
            1) Bienvenue
            2) Un message qui précise qu'on a 2 events partenaires (bars à jeux à Paris)
            3) 2 messages liés à 2 ConversationActivity (inputs)

    Notes :
      - conversation_activity_id_1 / _2 sont optionnels globalement,
        MAIS nécessaires pour le chemin "Paris" (<= 35km).
      - On récupère latitude/longitude depuis profil_winker.
        (Si tes champs ne s’appellent pas latitude/longitude, dis-moi leurs noms et je te le ajuste.)
    """
    ti = kwargs["ti"]
    targets = ti.xcom_pull(task_ids="get_targets", key="targets") or []
    if not targets:
        logging.info("Aucune cible à traiter.")
        ti.xcom_push(key="expo_tokens", value=[])
        return

    ca1, ca2 = _get_optional_conversation_activity_ids(kwargs)

    expo_tokens = []
    processed = 0
    sent_welcome_only = 0
    sent_paris_bundle = 0

    with _pg_connect() as connection:
        connection.autocommit = False
        try:
            with connection.cursor() as cur:
                # Si on doit envoyer les ConversationActivity, on valide qu’elles existent (fail fast)
                # MAIS uniquement si elles sont fournies — la validation stricte “obligatoire” se fait plus bas
                if ca1 is not None or ca2 is not None:
                    _assert_conversation_activities_exist(cur, ca1, ca2)

                for target_id in targets:
                    # Fetch user location
                    cur.execute(
                        """
                        SELECT lat, lon, "expoPushToken"
                        FROM profil_winker
                        WHERE id = %s
                        """,
                        (target_id,),
                    )
                    row = cur.fetchone()
                    if not row:
                        logging.warning("Winker %s introuvable, skip.", target_id)
                        continue

                    lat, lon, expo_token = row[0], row[1], row[2]

                    # Si pas de lat/lon, on considère "hors Paris" => welcome only (safe)
                    is_within_paris = False
                    if lat is not None and lon is not None:
                        try:
                            dist_km = _haversine_km(float(lat), float(lon), PARIS_LAT, PARIS_LON)
                            is_within_paris = dist_km <= PARIS_RADIUS_KM
                        except Exception:
                            logging.exception("Erreur calcul distance pour user %s (lat=%s lon=%s).", target_id, lat, lon)
                            is_within_paris = False

                    chat_id = _upsert_chat(cur, target_id)

                    if not is_within_paris:
                        # >35km (ou pas de géoloc) => seulement welcome
                        last_id = _insert_text_message(cur, chat_id, target_id, WELCOME_MESSAGE_TEXT)
                        _update_chat_last_and_unseen(cur, chat_id, target_id, last_msg_id=last_id, inc=1)
                        sent_welcome_only += 1
                    else:
                        # <=35km => bundle Paris (welcome + intro + 2 ConversationActivity)
                        if ca1 is None or ca2 is None:
                            raise ValueError(
                                "User within 35km of Paris requires both inputs: "
                                "conversation_activity_id_1 and conversation_activity_id_2"
                            )

                        id1 = _insert_text_message(cur, chat_id, target_id, WELCOME_MESSAGE_TEXT_SHORT)
                        id2 = _insert_text_message(cur, chat_id, target_id, PARIS_RECO_INTRO_TEXT)
                        id3 = _insert_conversation_activity_message(cur, chat_id, target_id, ca1)
                        id4 = _insert_conversation_activity_message(cur, chat_id, target_id, ca2)

                        # lastMessage = dernier message (2e conversation activity)
                        _update_chat_last_and_unseen(cur, chat_id, target_id, last_msg_id=id4, inc=4)
                        sent_paris_bundle += 1

                    if expo_token:
                        expo_tokens.append(expo_token)

                    processed += 1

            connection.commit()
            logging.info(
                "✅ Traitement terminé. processed=%s, welcome_only=%s, paris_bundle=%s",
                processed,
                sent_welcome_only,
                sent_paris_bundle,
            )
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
    description=(
        "Envoie un welcome à toutes les cibles. "
        "Si la cible est à <=35km de Paris, envoie aussi une reco + 2 ConversationActivity."
    ),
    default_args=default_args,
    schedule_interval=None,  # déclenchement manuel
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["chat", "broadcast", "expo", "116"],
    params={
        # Optionnels globalement; requis uniquement pour les users <=35km Paris
        "conversation_activity_id_1": Param(
            None, type=["null", "integer"], description="ConversationActivity id #1 (Paris bundle)"
        ),
        "conversation_activity_id_2": Param(
            None, type=["null", "integer"], description="ConversationActivity id #2 (Paris bundle)"
        ),
    },
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
