from __future__ import annotations

from datetime import datetime, date
from zoneinfo import ZoneInfo
import logging
import os
import json
from typing import Any, Dict, List, Optional

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from elasticsearch import Elasticsearch, helpers


PARIS_TZ = ZoneInfo("Europe/Paris")
DAG_ID = "sync_winker_to_elasticsearch"

# ✅ IMPORTANT : ton index final
INDEX_NAME = "nisu_winkers"

# Embeddings (optionnel mais recommandé)
EMBEDDINGS_URL = os.getenv("EMBEDDINGS_URL", "").strip()
EMBEDDINGS_TIMEOUT = int(os.getenv("EMBEDDINGS_TIMEOUT", "60"))
EMBEDDINGS_DIMS = int(os.getenv("EMBEDDINGS_DIMS", "768"))

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# ✅ DB: pas de champ is_banned -> on force FALSE
# ⚠️ Si certains champs n'existent pas (ex: comptePro, vectorPreferenceWinker...), enlève-les du SELECT.
SQL_SELECT_WINKERS = """
SELECT
    id,
    username,
    bio,
    "birthYear",
    city,
    region,
    subregion,
    "codePostal",
    pays,
    sexe,
    "comptePro",
    is_active,
    FALSE AS is_banned,
    lat,
    lon,
    "derniereRechercheEvent",
    "vectorPreferenceWinker",
    is_connected,
    last_connection
FROM profil_winker
WHERE is_active = TRUE;
"""


def get_es_client() -> Elasticsearch:
    """
    Client ES depuis la connexion Airflow `elasticsearch_default`.
    """
    conn = BaseHook.get_connection("elasticsearch_default")

    schema = conn.schema or "http"
    host = conn.host
    port = conn.port or 9200

    if conn.login and conn.password:
        url = f"{schema}://{conn.login}:{conn.password}@{host}:{port}"
    else:
        url = f"{schema}://{host}:{port}"

    extra = conn.extra_dejson or {}
    return Elasticsearch([url], **extra)


def safe_geo(lat: Any, lon: Any) -> Optional[Dict[str, float]]:
    if lat is None or lon is None:
        return None
    try:
        return {"lat": float(lat), "lon": float(lon)}
    except Exception:
        return None


def parse_vector16(raw: Any) -> Optional[List[float]]:
    """
    vectorPreferenceWinker : souvent une string JSON "[...]" de 16 floats.
    """
    if raw is None:
        return None
    try:
        if isinstance(raw, list):
            vec = raw
        else:
            s = str(raw).strip()
            if not s or s in ("null", "[]", "{}"):
                return None
            vec = json.loads(s)

        if not isinstance(vec, list) or len(vec) != 16:
            return None
        return [float(x) for x in vec]
    except Exception:
        return None


def build_profile_text(record: Dict[str, Any]) -> str:
    """
    Texte stable utilisé pour l'embedding KNN.
    """
    parts: List[str] = []

    bio = (record.get("bio") or "").strip()
    if bio:
        parts.append(bio)

    city = (record.get("city") or "").strip()
    subregion = (record.get("subregion") or "").strip()
    region = (record.get("region") or "").strip()
    loc = " ".join([p for p in [city, subregion, region] if p])
    if loc:
        parts.append(loc)

    last_search = (record.get("derniereRechercheEvent") or "").strip()
    if last_search and last_search not in ("{}", "[]", "null"):
        parts.append(last_search)

    return " | ".join(parts).strip()


def get_embedding(text: str) -> Optional[List[float]]:
    """
    Service embeddings (optionnel).
    Attend {"embedding":[...]} ou {"vector":[...]}.
    """
    if not EMBEDDINGS_URL:
        return None

    try:
        r = requests.post(
            EMBEDDINGS_URL,
            json={"text": text},
            timeout=EMBEDDINGS_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()

        vec = data.get("embedding") or data.get("vector")
        if not isinstance(vec, list) or not vec:
            return None

        out = [float(x) for x in vec]
        if len(out) != EMBEDDINGS_DIMS:
            logging.warning("Embedding dims inattendues: %s (attendu %s)", len(out), EMBEDDINGS_DIMS)
        return out
    except Exception as e:
        logging.warning("Embedding failed: %s", e)
        return None


def index_winkers_to_es(ti, **_):
    rows = ti.xcom_pull(task_ids="fetch_winkers_from_postgres") or []
    if not rows:
        logging.info("Aucun winker retourné par la requête SQL.")
        return

    col_names = [
        "id",
        "username",
        "bio",
        "birthYear",
        "city",
        "region",
        "subregion",
        "codePostal",
        "pays",
        "sexe",
        "comptePro",
        "is_active",
        "is_banned",
        "lat",
        "lon",
        "derniereRechercheEvent",
        "vectorPreferenceWinker",
        "is_connected",
        "last_connection",
    ]

    es = get_es_client()

    current_year = date.today().year
    now_iso = datetime.now(tz=PARIS_TZ).isoformat()

    actions: List[Dict[str, Any]] = []

    for row in rows:
        record = {col_names[i]: row[i] for i in range(len(col_names))}
        winker_id = int(record["id"])

        birth_year = record.get("birthYear")
        age = None
        if birth_year:
            try:
                age = current_year - int(birth_year)
            except Exception:
                age = None

        localisation = safe_geo(record.get("lat"), record.get("lon"))

        profile_text = build_profile_text(record)
        embedding_vector = get_embedding(profile_text) if profile_text else None
        vec16 = parse_vector16(record.get("vectorPreferenceWinker"))

        es_doc: Dict[str, Any] = {
            "winker_id": winker_id,
            "username": record.get("username"),

            "comptePro": bool(record.get("comptePro")),
            "is_active": bool(record.get("is_active")),
            "is_banned": bool(record.get("is_banned")),

            "sexe": record.get("sexe"),
            "birthYear": birth_year,
            "age": age,

            "city": record.get("city"),
            "region": record.get("region"),
            "subregion": record.get("subregion"),
            "codePostal": record.get("codePostal"),
            "pays": record.get("pays"),

            "bio": record.get("bio"),

            # si tu n’as pas encore de vraie liste de tags, laisse vide
            "preferences": [],

            "profile_text": profile_text or "",

            "is_connected": bool(record.get("is_connected")),
            "lastConnection": record.get("last_connection"),

            "derniereRechercheEvent": record.get("derniereRechercheEvent"),

            "boost": 0.0,
            "updated_at": now_iso,
        }

        if localisation:
            es_doc["localisation"] = localisation

        if vec16 is not None:
            es_doc["vectorPreferenceWinker"] = vec16

        if embedding_vector is not None:
            es_doc["embedding_vector"] = embedding_vector
        else:
            logging.info(
                "No embedding for winker_id=%s (profile_text empty or embeddings service unavailable).",
                winker_id,
            )

        actions.append(
            {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": str(winker_id),  # ✅ un doc par winker
                "_source": es_doc,
            }
        )

    if not actions:
        logging.info("Aucune action à envoyer vers Elasticsearch.")
        return

    logging.info("Indexation de %d winkers dans Elasticsearch (%s)...", len(actions), INDEX_NAME)
    helpers.bulk(es, actions, request_timeout=120)
    logging.info("Indexation terminée.")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["winker", "elasticsearch"],
) as dag:
    dag.timezone = PARIS_TZ

    fetch_winkers_from_postgres = PostgresOperator(
        task_id="fetch_winkers_from_postgres",
        postgres_conn_id="my_postgres",
        sql=SQL_SELECT_WINKERS,
        do_xcom_push=True,
    )

    index_winkers_to_elasticsearch = PythonOperator(
        task_id="index_winkers_to_elasticsearch",
        python_callable=index_winkers_to_es,
    )

    fetch_winkers_from_postgres >> index_winkers_to_elasticsearch
