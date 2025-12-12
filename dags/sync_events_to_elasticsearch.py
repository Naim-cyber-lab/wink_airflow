from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import json
from typing import Any, Dict, List, Optional

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook

from elasticsearch import Elasticsearch, helpers


PARIS_TZ = ZoneInfo("Europe/Paris")

DAG_ID = "sync_events_to_elasticsearch"
INDEX_NAME = "nisu_events"

# Ton endpoint FastAPI embeddings (à adapter)
# Exemple si ton API est sur la même machine :
EMBEDDINGS_URL = "https://recommendation.nisu.fr/api/v1/recommendations/embeddings"
EMBEDDINGS_TIMEOUT = 60

# Dimension attendue par ton mapping ES (768)
EMBEDDING_DIMS = 768

default_args = {
    "start_date": datetime(2024, 1, 1, tzinfo=PARIS_TZ),
    "retries": 1,
}

# ⚠️ ADAPTE CE SQL à ton vrai schéma.
# Idées de colonnes typiques :
# - id
# - winker_id / "winkerId"
# - titre / title
# - bio / description
# - preferences (json / array / text)
# - lat / lon
# - boost (int)
SQL_SELECT_EVENTS = """
SELECT
    id,
    -- adapte selon ton schéma: winker_id ou "winkerId"
    winker_id,
    titre,
    bio,
    preferences,
    boost,
    lat,
    lon
FROM profil_event;
"""


def get_es_client() -> Elasticsearch:
    """
    Construit un client Elasticsearch via la connexion Airflow `elasticsearch_default`.
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
    Crée l'index `nisu_events` si absent (mapping identique à ce que tu as créé).
    """
    if es.indices.exists(index=INDEX_NAME):
        logging.info("Index '%s' existe déjà, pas de création.", INDEX_NAME)
        return

    body = {
        "mappings": {
            "properties": {
                "winkerId": {"type": "keyword"},
                "boost": {"type": "integer"},
                "localisation": {"type": "geo_point"},

                "bio": {"type": "text"},
                "bio_vector": {
                    "type": "dense_vector",
                    "dims": EMBEDDING_DIMS,
                    "index": True,
                    "similarity": "cosine",
                },

                "titre": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}},
                },
                "titre_vector": {
                    "type": "dense_vector",
                    "dims": EMBEDDING_DIMS,
                    "index": True,
                    "similarity": "cosine",
                },

                "preferences": {"type": "keyword"},
                "preferences_vector": {
                    "type": "dense_vector",
                    "dims": EMBEDDING_DIMS,
                    "index": True,
                    "similarity": "cosine",
                },

                "embedding_vector": {
                    "type": "dense_vector",
                    "dims": EMBEDDING_DIMS,
                    "index": True,
                    "similarity": "cosine",
                },
            }
        }
    }

    es.indices.create(index=INDEX_NAME, body=body)
    logging.info("Index '%s' créé.", INDEX_NAME)


def _parse_preferences(value: Any) -> List[str]:
    """
    Essaie de normaliser preferences en List[str].
    - accepte: None, list, tuple, set
    - accepte: JSON string (ex: '["a","b"]')
    - accepte: string simple 'a,b,c'
    - accepte: format postgres array '{a,b,c}'
    """
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        return [str(x).strip() for x in value if str(x).strip()]

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []

        # JSON list ?
        if (s.startswith("[") and s.endswith("]")):
            try:
                data = json.loads(s)
                if isinstance(data, list):
                    return [str(x).strip() for x in data if str(x).strip()]
            except Exception:
                pass

        # Postgres array style: {a,b,c}
        if s.startswith("{") and s.endswith("}"):
            inner = s[1:-1].strip()
            if not inner:
                return []
            return [x.strip().strip('"') for x in inner.split(",") if x.strip()]

        # CSV simple
        if "," in s:
            return [x.strip() for x in s.split(",") if x.strip()]

        # string unique
        return [s]

    return [str(value).strip()] if str(value).strip() else []


def _embedding(text: str) -> List[float]:
    """
    Appelle ton endpoint FastAPI /embeddings et renvoie embedding (List[float]).
    """
    if not text:
        return []

    r = requests.post(
        EMBEDDINGS_URL,
        json={"text": text},
        timeout=EMBEDDINGS_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()

    vec = data.get("embedding") or []
    if not isinstance(vec, list):
        return []

    # Optionnel: on sécurise la dimension
    if len(vec) != EMBEDDING_DIMS:
        logging.warning("Embedding dimension mismatch: got=%s expected=%s", len(vec), EMBEDDING_DIMS)
        # tu peux choisir de raise ici si tu veux
    return vec


def index_events_to_es(ti, **_):
    rows = ti.xcom_pull(task_ids="fetch_events_from_postgres") or []
    if not rows:
        logging.info("Aucun event retourné par la requête SQL.")
        return

    # ⚠️ doit correspondre à l'ordre du SELECT
    col_names = [
        "id",
        "winker_id",
        "titre",
        "bio",
        "preferences",
        "boost",
        "lat",
        "lon",
    ]

    es = get_es_client()
    create_index_if_needed(es)

    actions = []

    for row in rows:
        record = {col_names[i]: row[i] for i in range(len(col_names))}

        event_id = record["id"]
        winker_id = record.get("winker_id")
        titre = record.get("titre") or ""
        bio = record.get("bio") or ""
        preferences = _parse_preferences(record.get("preferences"))
        boost = record.get("boost")

        # geo_point
        lat = record.get("lat")
        lon = record.get("lon")
        localisation = None
        if lat is not None and lon is not None:
            try:
                localisation = {"lat": float(lat), "lon": float(lon)}
            except Exception:
                localisation = None

        # Texte fusionné pour embedding_vector
        preferences_text = ", ".join(preferences) if preferences else ""
        merged_text = " ".join([x for x in [titre, bio, preferences_text] if x]).strip()

        # Embeddings (tu peux désactiver certains si tu veux accélérer)
        try:
            titre_vec = _embedding(titre) if titre else []
            bio_vec = _embedding(bio) if bio else []
            pref_vec = _embedding(preferences_text) if preferences_text else []
            merged_vec = _embedding(merged_text) if merged_text else []
        except Exception as e:
            logging.exception("Erreur embeddings pour event_id=%s: %s", event_id, e)
            # selon ton besoin: skip ou index sans vecteurs
            titre_vec, bio_vec, pref_vec, merged_vec = [], [], [], []

        es_doc: Dict[str, Any] = {
            "winkerId": str(winker_id) if winker_id is not None else None,
            "boost": int(boost) if boost is not None else 0,
            "titre": titre,
            "bio": bio,
            "preferences": preferences,
        }

        if localisation:
            es_doc["localisation"] = localisation

        # On n’envoie les vecteurs que s’ils existent (sinon ES peut refuser selon version/settings)
        if bio_vec:
            es_doc["bio_vector"] = bio_vec
        if titre_vec:
            es_doc["titre_vector"] = titre_vec
        if pref_vec:
            es_doc["preferences_vector"] = pref_vec
        if merged_vec:
            es_doc["embedding_vector"] = merged_vec

        actions.append(
            {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": event_id,      # pas de doublons
                "_source": es_doc,
            }
        )

    if not actions:
        logging.info("Aucune action à envoyer vers Elasticsearch.")
        return

    logging.info("Indexation de %d events dans Elasticsearch...", len(actions))
    helpers.bulk(es, actions)
    logging.info("Indexation terminée.")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["events", "elasticsearch"],
) as dag:
    dag.timezone = PARIS_TZ

    fetch_events_from_postgres = PostgresOperator(
        task_id="fetch_events_from_postgres",
        postgres_conn_id="my_postgres",
        sql=SQL_SELECT_EVENTS,
        do_xcom_push=True,
    )

    index_events_to_elasticsearch = PythonOperator(
        task_id="index_events_to_elasticsearch",
        python_callable=index_events_to_es,
    )

    fetch_events_from_postgres >> index_events_to_elasticsearch
