from __future__ import annotations

from datetime import datetime, date
from zoneinfo import ZoneInfo
import logging
import os
import json
from typing import Any, Dict, List, Optional, Sequence

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from elasticsearch import Elasticsearch, helpers


# -------------------------
# Config
# -------------------------
PARIS_TZ = ZoneInfo("Europe/Paris")
DAG_ID = "sync_winker_to_elasticsearch"
INDEX_NAME = "nisu_winkers"

# Embeddings (optionnel mais recommandé)
EMBEDDINGS_URL = os.getenv("EMBEDDINGS_URL", "").strip()
EMBEDDINGS_TIMEOUT = int(os.getenv("EMBEDDINGS_TIMEOUT", "60"))
EMBEDDINGS_DIMS = int(os.getenv("EMBEDDINGS_DIMS", "768"))

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# ⚠️ colonnes camelCase -> guillemets
# ⚠️ Si "is_banned" n'existe PAS en DB : remplace COALESCE(is_banned, FALSE) par FALSE AS is_banned
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
    comptePro,
    is_active,
    COALESCE(is_banned, FALSE) AS is_banned,
    lat,
    lon,
    derniereRechercheEvent,
    vectorPreferenceWinker,
    is_connected,
    last_connection
FROM profil_winker
WHERE is_active = TRUE;
"""


# -------------------------
# Elasticsearch helpers
# -------------------------
def get_es_client() -> Elasticsearch:
    """
    Récupère la connexion Airflow 'elasticsearch_default' et instancie le client ES.
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


def create_index_if_needed(es: Elasticsearch) -> None:
    """
    Crée l'index ES avec TON mapping s'il n'existe pas.
    Fix principal: configure l'analyzer text_fr (sinon BadRequestError).
    """
    if es.indices.exists(index=INDEX_NAME):
        logging.info("Index '%s' existe déjà.", INDEX_NAME)
        return

    logging.info("Index '%s' absent -> création.", INDEX_NAME)

    body: Dict[str, Any] = {
        "settings": {
            "index": {"number_of_shards": 1, "number_of_replicas": 0},
            "analysis": {
                "normalizer": {
                    "lc_norm": {
                        "type": "custom",
                        "filter": ["lowercase", "asciifolding"],
                    }
                },
                # ✅ Fix: si un champ référence analyzer "text_fr", ES doit le connaître
                "analyzer": {
                    "text_fr": {"type": "french"}
                },
            },
        },
        "mappings": {
            "dynamic": "false",
            "properties": {
                "winker_id": {"type": "integer"},
                "username": {"type": "keyword", "normalizer": "lc_norm"},

                "comptePro": {"type": "boolean"},
                "is_active": {"type": "boolean"},
                "is_banned": {"type": "boolean"},

                "sexe": {"type": "keyword"},
                "age": {"type": "short"},
                "birthYear": {"type": "short"},

                "city": {"type": "keyword", "normalizer": "lc_norm"},
                "region": {"type": "keyword", "normalizer": "lc_norm"},
                "subregion": {"type": "keyword", "normalizer": "lc_norm"},
                "pays": {"type": "keyword", "normalizer": "lc_norm"},
                "codePostal": {"type": "keyword"},

                "localisation": {"type": "geo_point"},

                # Tu peux mettre analyzer "text_fr" si tu veux (pas obligatoire)
                "bio": {"type": "text"},
                "derniereRechercheEvent": {"type": "text"},
                "profile_text": {"type": "text"},

                "preferences": {"type": "keyword"},
                "vectorPreferenceWinker": {"type": "dense_vector", "dims": 16, "index": False},

                "embedding_vector": {
                    "type": "dense_vector",
                    "dims": EMBEDDINGS_DIMS,
                    "index": True,
                    "similarity": "cosine",
                },

                "is_connected": {"type": "boolean"},
                "lastConnection": {"type": "date"},

                "boost": {"type": "float"},

                # ✅ champ propre
                "updated_at": {"type": "date"},
            },
        },
    }

    es.indices.create(index=INDEX_NAME, body=body)
    logging.info("Index '%s' créé.", INDEX_NAME)


# -------------------------
# Data helpers
# -------------------------
def safe_geo(lat: Any, lon: Any) -> Optional[Dict[str, float]]:
    if lat is None or lon is None:
        return None
    try:
        return {"lat": float(lat), "lon": float(lon)}
    except Exception:
        return None


def parse_vector16(raw: Any) -> Optional[List[float]]:
    """
    vectorPreferenceWinker est stocké comme TextField (JSON string) en DB.
    Convertit en list[float] (len=16) si possible.
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
    Texte stable utilisé pour calculer embedding_vector.
    """
    parts: List[str] = []

    bio = (record.get("bio") or "").strip()
    if bio:
        parts.append(bio)

    # localisation (signal faible)
    city = (record.get("city") or "").strip()
    subregion = (record.get("subregion") or "").strip()
    region = (record.get("region") or "").strip()
    loc = " ".join([p for p in [city, subregion, region] if p])
    if loc:
        parts.append(loc)

    # historique recherche event (signal fort)
    last_search = (record.get("derniereRechercheEvent") or "").strip()
    if last_search and last_search not in ("{}", "[]", "null"):
        parts.append(last_search)

    return " | ".join(parts).strip()


def get_embedding(text: str) -> Optional[List[float]]:
    """
    Appel optionnel à un service d'embeddings.
    Attend une réponse: {"embedding":[...]} ou {"vector":[...]}.
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
            logging.warning(
                "Embedding dims inattendues: %s (attendu %s)",
                len(out),
                EMBEDDINGS_DIMS,
            )
        return out
    except Exception as e:
        logging.warning("Embedding failed: %s", e)
        return None


# -------------------------
# Main indexing task
# -------------------------
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
    create_index_if_needed(es)

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

            # Si tu n’as pas encore de vraie liste de tags: laisse vide
            "preferences": [],

            "profile_text": profile_text or "",

            "is_connected": bool(record.get("is_connected")),
            # DB -> last_connection, ES -> lastConnection
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
            # pas bloquant, mais sans ça pas de KNN
            logging.info(
                "No embedding for winker_id=%s (profile_text empty or embeddings service unavailable).",
                winker_id,
            )

        actions.append(
            {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": str(winker_id),
                "_source": es_doc,
            }
        )

    if not actions:
        logging.info("Aucune action à envoyer vers Elasticsearch.")
        return

    logging.info("Indexation de %d winkers dans Elasticsearch (%s)...", len(actions), INDEX_NAME)
    helpers.bulk(es, actions, request_timeout=120)
    logging.info("Indexation terminée.")


# -------------------------
# DAG
# -------------------------
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
