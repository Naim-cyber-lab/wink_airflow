from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import json
from typing import Any, Dict, List

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook

from elasticsearch import Elasticsearch, helpers


PARIS_TZ = ZoneInfo("Europe/Paris")

DAG_ID = "sync_events_to_elasticsearch"
# ✅ index dédié reco pour NE PAS écraser l’index canonique "nisu_events"
INDEX_NAME = "nisu_events_reco"

# endpoint embeddings (si 404 -> on continue sans embeddings)
EMBEDDINGS_URL = "https://recommendation.nisu.fr/api/v1/recommendations/embeddings"
EMBEDDINGS_TIMEOUT = 60
EMBEDDING_DIMS = 768

default_args = {
    "start_date": datetime(2024, 1, 1, tzinfo=PARIS_TZ),
    "retries": 1,
}

SQL_SELECT_EVENTS = """
SELECT
  id,
  "creatorWinker_id" AS winker_id,
  titre,
  "bioEvent" AS bio,
  "hastagEvents" AS preferences,
  0 AS boost,
  lat,
  lon
FROM profil_event;
"""


def get_es_client() -> Elasticsearch:
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


def _parse_preferences(value: Any) -> List[str]:
    """
    hastagEvents: parfois string, parfois JSON, parfois CSV.
    On normalise en list[str].
    """
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        return [str(x).strip().lstrip("#") for x in value if str(x).strip()]

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []

        # JSON list ?
        if s.startswith("[") and s.endswith("]"):
            try:
                data = json.loads(s)
                if isinstance(data, list):
                    return [str(x).strip().lstrip("#") for x in data if str(x).strip()]
            except Exception:
                pass

        # CSV ?
        if "," in s:
            return [x.strip().lstrip("#") for x in s.split(",") if x.strip()]

        # hashtags séparés par espaces
        if " " in s and "#" in s:
            parts = [p.strip() for p in s.split(" ") if p.strip()]
            return [p.lstrip("#") for p in parts]

        return [s.lstrip("#")]

    s = str(value).strip()
    return [s.lstrip("#")] if s else []


def _embedding(text: str) -> List[float]:
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

    if len(vec) != EMBEDDING_DIMS:
        logging.warning("Embedding dims mismatch: got=%s expected=%s", len(vec), EMBEDDING_DIMS)

    return vec


def index_events_to_es(ti, **_):
    rows = ti.xcom_pull(task_ids="fetch_events_from_postgres") or []
    if not rows:
        logging.info("Aucun event à indexer.")
        return

    col_names = ["id", "winker_id", "titre", "bio", "preferences", "boost", "lat", "lon"]
    es = get_es_client()

    actions = []

    for row in rows:
        rec = {col_names[i]: row[i] for i in range(len(col_names))}

        event_id = rec["id"]  # ✅ id profil_event
        winker_id = rec.get("winker_id")
        titre = rec.get("titre") or ""
        bio = rec.get("bio") or ""
        preferences = _parse_preferences(rec.get("preferences"))
        boost = rec.get("boost") or 0

        lat = rec.get("lat")
        lon = rec.get("lon")
        localisation = None
        if lat is not None and lon is not None:
            try:
                localisation = {"lat": float(lat), "lon": float(lon)}
            except Exception:
                localisation = None

        preferences_text = ", ".join(preferences) if preferences else ""
        merged_text = " ".join([x for x in [titre, bio, preferences_text] if x]).strip()

        # ✅ embeddings : si 404 / erreur -> on ne casse pas l’indexation
        merged_vec: List[float] = []
        if merged_text:
            try:
                merged_vec = _embedding(merged_text)
            except Exception as e:
                logging.exception("Erreur embedding event_id=%s: %s", event_id, e)
                merged_vec = []

        # ✅ doc RECO (dans index dédié)
        doc: Dict[str, Any] = {
            "event_id": str(event_id),  # ✅ champ existant, requêtable via exists
            "winkerId": str(winker_id) if winker_id is not None else None,
            "boost": int(boost),
            "titre": titre,
            "bio": bio,
            "preferences": preferences,
        }

        if localisation:
            doc["localisation"] = localisation

        if merged_vec:
            doc["embedding_vector"] = merged_vec

        actions.append(
            {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": str(event_id),  # ✅ ES _id = id profil_event
                "_source": doc,
            }
        )

    helpers.bulk(es, actions)
    logging.info("Indexation terminée: %d docs -> index=%s", len(actions), INDEX_NAME)


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["events", "elasticsearch", "reco"],
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
