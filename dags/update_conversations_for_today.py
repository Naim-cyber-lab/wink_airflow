# dags/update_conversations_for_today.py
import os
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg2
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ---------- CONFIG ----------

DB_CONN_ID = "my_postgres"  # Airflow Connection -> Postgres
PARIS_TZ = ZoneInfo("Europe/Paris")

# Table de paramétrage:
#   profil_nisu_param_config(perimeter, config_one=region, config_two=target/nb)
# -> filtre par perimeter ci-dessous (adapte si ton libellé diffère)
PARAM_PERIMETER = "conversation"

# Expo Push
EXPO_API = "https://exp.host/--/api/v2/push/send"
# Configure en Variable Airflow (Admin -> Variables) ou en env
EXPO_ACCESS_TOKEN = Variable.get("EXPO_ACCESS_TOKEN", default_var=os.getenv("EXPO_ACCESS_TOKEN", ""))

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------- HELPERS SQL / EXPO ----------

def _pg_connect():
    """Connexion Postgres via Airflow Connection."""
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )

def _send_expo(tokens, title, body, data=None):
    """Envoie des push Expo par paquets de 100."""
    # Filtrer les tokens valides Expo
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
    if EXPO_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {EXPO_ACCESS_TOKEN}"

    sent = 0
    for i in range(0, len(messages), 100):
        chunk = messages[i : i + 100]
        try:
            r = requests.post(EXPO_API, json=chunk, headers=headers, timeout=15)
            r.raise_for_status()
            res = r.json()
            # Log des erreurs renvoyées par Expo
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

# ---------- TASKS ----------

def get_targets(**kwargs):
    """
    Lit les objectifs quotidiens (nb de conversations à publier par région) dans
    profil_nisu_param_config où perimeter = PARAM_PERIMETER.
    - config_one = code région (même format que ConversationActivity.region)
    - config_two = target (int)
    """
    ti = kwargs["ti"]
    with _pg_connect() as connection, connection.cursor() as cur:
        cur.execute(
            """
                SELECT config_one AS region, COALESCE(CAST(float_param AS INTEGER), 0) AS target
                FROM profil_nisu_param_config
                WHERE perimeter = %s
            """,
            (PARAM_PERIMETER,),
        )
        targets = {region: int(target or 0) for region, target in cur.fetchall()}
    logging.info("🎯 Targets (conversation/jour) : %s", targets)
    ti.xcom_push(key="targets", value=targets)

def get_published_today(**kwargs):
    """
    Compte les conversations déjà publiées aujourd'hui par région.
    """
    ti = kwargs["ti"]
    today = datetime.now(PARIS_TZ).date()

    with _pg_connect() as connection, connection.cursor() as cur:
        cur.execute(
            """
            SELECT c.region, COUNT(*)::int AS cnt
            FROM profil_conversationactivity c
            WHERE c."datePublication" = %s
            GROUP BY c.region
            """,
            (today,),
        )
        rows = cur.fetchall()
        published = {region: cnt for region, cnt in rows}
    logging.info("📊 Publiées aujourd'hui : %s", published)
    ti.xcom_push(key="published", value=published)

def compute_missing(**kwargs):
    """
    Calcule, pour chaque région connue dans targets, le nombre à compléter aujourd'hui.
    """
    ti = kwargs["ti"]
    targets = ti.xcom_pull(task_ids="get_targets", key="targets") or {}
    published = ti.xcom_pull(task_ids="get_published_today", key="published") or {}

    missing = {}
    for region, tgt in targets.items():
        cur_cnt = int(published.get(region, 0))
        miss = max(tgt - cur_cnt, 0)
        if miss > 0:
            missing[region] = miss

    logging.info("🧮 Manquants par région : %s", missing)
    ti.xcom_push(key="missing", value=missing)

def rotate_and_update_for_today(**kwargs):
    """
    Assigne datePublication = aujourd'hui à des conversations candidates pour chaque région
    afin d'atteindre l'objectif du jour.
    Renvoie en XCom un dict { region: nb_mises_a_jour }.
    """
    ti = kwargs["ti"]
    missing = ti.xcom_pull(task_ids="compute_missing", key="missing") or {}
    today = datetime.now(PARIS_TZ).date()

    if not missing:
        logging.info("✅ Aucun complément nécessaire.")
        ti.xcom_push(key="updated", value={})
        return

    updated_per_region = {}

    with _pg_connect() as connection, connection.cursor() as cur:
        total = 0
        for region, to_add in missing.items():
            logging.info("↪️ Région %s : besoin de %s", region, to_add)

            # Candidats = conversations de la région dont la datePublication != aujourd'hui
            # On prend les plus “anciennes” d'abord (NULL/anciennes en premier)
            cur.execute(
                """
                SELECT id
                FROM profil_conversationactivity
                WHERE region = %s
                  AND ("datePublication" IS DISTINCT FROM %s)
                ORDER BY COALESCE("datePublication", DATE '1900-01-01') ASC, id ASC
                LIMIT %s
                """,
                (region, today, to_add),
            )
            ids = [row[0] for row in cur.fetchall()]
            if not ids:
                logging.info("… Pas de candidats pour %s", region)
                continue

            cur.execute(
                """
                UPDATE profil_conversationactivity
                SET "datePublication" = %s
                WHERE id = ANY(%s)
                """,
                (today, ids),
            )
            n = cur.rowcount
            total += n
            updated_per_region[region] = updated_per_region.get(region, 0) + n
            logging.info("✅ %s conversations mises à aujourd'hui (%s)", n, region)

        connection.commit()
        logging.info("🎉 Total mises à aujourd'hui: %s", total)

    ti.xcom_push(key="updated", value=updated_per_region)

def notify_users_for_regions(**kwargs):
    """
    Pour chaque région mise à jour, envoie un push aux Winkers de cette région
    (champ winker.expoPushToken) pour dire que de nouvelles conversations sont dispo.
    """
    ti = kwargs["ti"]
    updated = ti.xcom_pull(task_ids="rotate_and_update_for_today", key="updated") or {}
    if not updated:
        logging.info("🔕 Aucune région mise à jour : pas de push.")
        return

    with _pg_connect() as connection, connection.cursor() as cur:
        for region, count in updated.items():
            if count <= 0:
                continue

            # Récupérer les tokens des winkers de la région
            cur.execute(
                """
                SELECT "expoPushToken"
                FROM profil_winker
                WHERE region = %s
                  AND "expoPushToken" IS NOT NULL
                  AND "expoPushToken" <> ''
                """,
                (region,),
            )
            tokens = [row[0] for row in cur.fetchall()]
            if not tokens:
                logging.info("ℹ️ Aucun token Expo pour la région %s", region)
                continue

            title = "Nouvelles conversations 📣"
            body = f"{count} nouvelles conversations disponibles aujourd'hui dans ta région."
            payload = {"type": "conversation_refresh", "region": region, "count": int(count)}
            sent = _send_expo(tokens, title, body, data=payload)
            logging.info("📲 Push envoyé à %s utilisateurs (%s)", sent, region)

# ---------- DAG ----------

with DAG(
    dag_id="update_conversations_for_today",
    default_args=default_args,
    description="Fixe les conversations du jour par région et notifie les utilisateurs (Expo).",
    schedule_interval="0 7 * * *",  # tous les jours à 07:00 (serveur). Adapte si besoin.
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["conversations", "push", "expo"],
) as dag:

    t1 = PythonOperator(
        task_id="get_targets",
        python_callable=get_targets,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="get_published_today",
        python_callable=get_published_today,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="compute_missing",
        python_callable=compute_missing,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="rotate_and_update_for_today",
        python_callable=rotate_and_update_for_today,
        provide_context=True,
    )

    t5 = PythonOperator(
        task_id="notify_users",
        python_callable=notify_users_for_regions,
        provide_context=True,
    )

    [t1, t2] >> t3 >> t4 >> t5
