from zoneinfo import ZoneInfo
from datetime import datetime
import json
import logging
import os
from typing import Any, List, Dict

import requests
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

PARIS_TZ = ZoneInfo("Europe/Paris")

# =====================================================================
# Config de base
# =====================================================================

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

DAG_ID = "sync_postgres_to_es_winkers_events"

# Exécution quotidienne à 3h du matin (Paris)
SCHEDULE_CRON = "0 3 * * *"

# Connexion Postgres (à adapter à ton conn_id Airflow)
POSTGRES_CONN_ID = "my_postgres"

# URL de ton service FastAPI de reco (à adapter)
# Exemple si tu es en local :
# FASTAPI_BASE_URL = "http://localhost:8000"
FASTAPI_BASE_URL = os.environ.get("NISU_RECO_API_URL", "http://nisu-recommendation:8000")

# =====================================================================
# SQL : à adapter selon ton schéma / vues
# L'idée : tu peux créer des VUES déjà "propres" pour ES.
# =====================================================================

SQL_SELECT_WINKERS_FOR_ES = """
SELECT
    id,
    username,
    email,
    sexe,
    age,
    city,
    region,
    subregion,
    pays,
    latitude,           -- à adapter: lat
    longitude,          -- à adapter: lon
    visible_tags,       -- ex: JSON ou ARRAY
    preference_vector,  -- ex: JSON ou ARRAY (16 dims)
    meet_eligible,
    mails_eligible
FROM profil_winker_for_es_sync;
"""

SQL_SELECT_EVENTS_FOR_ES = """
SELECT
    id,
    titre,
    bio_event,
    city,
    region,
    subregion,
    pays,
    code_postal,
    latitude,               -- lat
    longitude,              -- lon
    date_event,
    date_publication,
    age_minimum,
    age_maximum,
    access_fille,
    access_garcon,
    access_tous,
    hashtag_events,         -- JSON ou ARRAY
    meet_eligible,
    plan_trip_elligible,
    current_nb_participants,
    max_number_participant,
    is_full,
    vector_preference_event -- JSON ou ARRAY (16 dims)
FROM profil_event_for_es_sync;
"""

# =====================================================================
# Helpers Python
# =====================================================================

def _convert_pg_value(val: Any) -> Any:
    """
    Petits helpers pour gérer les colonnes JSON / ARRAY renvoyées par Postgres.
    Si c'est une string qui ressemble à du JSON -> json.loads.
    Sinon on renvoie tel quel.
    """
    if isinstance(val, str):
        val_strip = val.strip()
        if (val_strip.startswith("[") and val_strip.endswith("]")) or \
           (val_strip.startswith("{") and val_strip.endswith("}")):
            try:
                return json.loads(val_strip)
            except Exception:
                return val  # on laisse en l'état si parse impossible
    return val


def build_winkers_payload(rows: List[tuple]) -> List[Dict[str, Any]]:
    """
    Transforme les lignes SQL (tuples) en liste de dictionnaires
    conformes au schéma WinkerIn de ton service FastAPI.
    """
    payload = []
    for row in rows:
        (
            id_,
            username,
            email,
            sexe,
            age,
            city,
            region,
            subregion,
            pays,
            lat,
            lon,
            visible_tags,
            preference_vector,
            meet_eligible,
            mails_eligible,
        ) = row

        payload.append(
            {
                "id": id_,
                "username": username,
                "email": email,
                "sexe": sexe,
                "age": age,
                "city": city,
                "region": region,
                "subregion": subregion,
                "pays": pays,
                "lat": lat,
                "lon": lon,
                "visible_tags": _convert_pg_value(visible_tags) or [],
                "preference_vector": _convert_pg_value(preference_vector),
                "meet_eligible": meet_eligible,
                "mails_eligible": mails_eligible,
            }
        )
    return payload


def build_events_payload(rows: List[tuple]) -> List[Dict[str, Any]]:
    """
    Transforme les lignes SQL (tuples) en liste de dictionnaires
    conformes au schéma EventIn de ton service FastAPI.
    """
    payload = []
    for row in rows:
        (
            id_,
            titre,
            bio_event,
            city,
            region,
            subregion,
            pays,
            code_postal,
            lat,
            lon,
            date_event,
            date_publication,
            age_minimum,
            age_maximum,
            access_fille,
            access_garcon,
            access_tous,
            hashtag_events,
            meet_eligible,
            plan_trip_elligible,
            current_nb_participants,
            max_number_participant,
            is_full,
            vector_preference_event,
        ) = row

        payload.append(
            {
                "id": id_,
                "titre": titre,
                "bioEvent": bio_event,
                "city": city,
                "region": region,
                "subregion": subregion,
                "pays": pays,
                "codePostal": code_postal,
                "lat": lat,
                "lon": lon,
                "dateEvent": date_event.isoformat() if date_event else None,
                "datePublication": date_publication.isoformat() if date_publication else None,
                "ageMinimum": age_minimum,
                "ageMaximum": age_maximum,
                "accessFille": access_fille,
                "accessGarcon": access_garcon,
                "accessTous": access_tous,
                "hastagEvents": _convert_pg_value(hashtag_events) or [],
                "meetEligible": meet_eligible,
                "planTripElligible": plan_trip_elligible,
                "currentNbParticipants": current_nb_participants,
                "maxNumberParticipant": max_number_participant,
                "isFull": is_full,
                "vectorPreferenceEvent": _convert_pg_value(vector_preference_event),
            }
        )
    return payload


def push_winkers_to_es(ti, **_):
    """
    Récupère les lignes retournées par le PostgresOperator (XCom),
    les transforme en JSON, puis les envoie au service FastAPI
    sur /api/v1/index/winkers/bulk.
    """
    rows = ti.xcom_pull(task_ids="fetch_winkers_task") or []
    logging.info(f"[WINKERS] {len(rows)} rows fetched from Postgres")

    payload = build_winkers_payload(rows)

    if not payload:
        logging.info("[WINKERS] Nothing to send to ES")
        return

    url = f"{FASTAPI_BASE_URL}/api/v1/index/winkers/bulk"
    logging.info(f"[WINKERS] Sending {len(payload)} documents to {url}")

    resp = requests.post(url, json=payload, timeout=60)
    resp.raise_for_status()

    logging.info(f"[WINKERS] ES index response: {resp.status_code} - {resp.text}")


def push_events_to_es(ti, **_):
    """
    Idem pour les events.
    """
    rows = ti.xcom_pull(task_ids="fetch_events_task") or []
    logging.info(f"[EVENTS] {len(rows)} rows fetched from Postgres")

    payload = build_events_payload(rows)

    if not payload:
        logging.info("[EVENTS] Nothing to send to ES")
        return

    url = f"{FASTAPI_BASE_URL}/api/v1/index/events/bulk"
    logging.info(f"[EVENTS] Sending {len(payload)} documents to {url}")

    resp = requests.post(url, json=payload, timeout=60)
    resp.raise_for_status()

    logging.info(f"[EVENTS] ES index response: {resp.status_code} - {resp.text}")


# =====================================================================
# Définition du DAG
# =====================================================================

with DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_CRON,
    default_args=default_args,
    catchup=False,
    tags=["sync", "elasticsearch", "winker", "event"],
) as dag:
    dag.timezone = PARIS_TZ

    # 1) Récupérer les winkers à indexer
    fetch_winkers_task = PostgresOperator(
        task_id="fetch_winkers_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_SELECT_WINKERS_FOR_ES,
        do_xcom_push=True,  # on récupère les résultats dans XCom
    )

    # 2) Pousser les winkers dans ES via FastAPI
    push_winkers_task = PythonOperator(
        task_id="push_winkers_to_es",
        python_callable=push_winkers_to_es,
    )

    # 3) Récupérer les events à indexer
    fetch_events_task = PostgresOperator(
        task_id="fetch_events_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_SELECT_EVENTS_FOR_ES,
        do_xcom_push=True,
    )

    # 4) Pousser les events dans ES via FastAPI
    push_events_task = PythonOperator(
        task_id="push_events_to_es",
        python_callable=push_events_to_es,
    )

    # Ordonnancement : on fait tout, mais winkers et events peuvent être parallèles
    fetch_winkers_task >> push_winkers_task
    fetch_events_task >> push_events_task
