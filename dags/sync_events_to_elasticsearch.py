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

# Endpoint embeddings (Swagger route GET /embedding?text=...)
EMBEDDINGS_URL = "https://recommendation.nisu.fr/api/v1/recommendations/embedding"
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
  titre_fr,
  "bioEvent" AS bio,
  "bioEvent_fr" AS bio_fr,
  "hastagEvents" AS preferences,
  0 AS boost,
  lat,
  lon
FROM profil_event;
"""

# Doit matcher l'ordre exact du SELECT ci-dessus (10 colonnes)
COL_NAMES = [
    "id",
    "winker_id",
    "titre",
    "titre_fr",
    "bio",
    "bio_fr",
    "preferences",
    "boost",
    "lat",
    "lon",
]


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
    On normalise en list[str] sans "#".
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
    """
    Appelle le service FastAPI exposé dans Swagger:
    GET /embedding?text=...
    Retour attendu: { "dims": 768, "embedding": [...], "normalized": true }
    """
    if not text:
        return []

    r = requests.get(
        EMBEDDINGS_URL,
        params={"text": text},
        timeout=EMBEDDINGS_TIMEOUT,
        headers={"accept": "application/json"},
    )
    r.raise_for_status()

    data = r.json() or {}
    vec = data.get("embedding") or []
    if not isinstance(vec, list):
        return []

    if len(vec) != EMBEDDING_DIMS:
        logging.warning(
            "Embedding dims mismatch: got=%s expected=%s",
            len(vec),
            EMBEDDING_DIMS,
        )

    return vec


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _build_localisation(lat: Any, lon: Any) -> Optional[Dict[str, float]]:
    if lat is None or lon is None:
        return None
    try:
        return {"lat": float(lat), "lon": float(lon)}
    except Exception:
        return None


def index_events_to_es(ti, **_):
    rows = ti.xcom_pull(task_ids="fetch_events_from_postgres") or []
    if not rows:
        logging.info("Aucun event à indexer.")
        return

    logging.info("Nombre total de rows récupérées: %d", len(rows))

    first_row = rows[0]
    logging.info("Première row (nb colonnes=%d): %s", len(first_row), first_row)

    # petit check d'alignement (debug)
    if len(first_row) != len(COL_NAMES):
        logging.warning(
            "Mismatch colonnes: SELECT renvoie %d colonnes mais COL_NAMES en contient %d",
            len(first_row),
            len(COL_NAMES),
        )

    es = get_es_client()
    actions: List[Dict[str, Any]] = []

    for row in rows:
        # mapping safe: s'arrête au min des deux longueurs
        m = min(len(COL_NAMES), len(row))
        rec = {COL_NAMES[i]: row[i] for i in range(m)}

        event_id = rec.get("id")
        winker_id = rec.get("winker_id")

        titre = (rec.get("titre") or "").strip()
        titre_fr = (rec.get("titre_fr") or "").strip()
        bio = (rec.get("bio") or "").strip()
        bio_fr = (rec.get("bio_fr") or "").strip()

        preferences = _parse_preferences(rec.get("preferences"))
        boost = _safe_int(rec.get("boost"), default=0)

        localisation = _build_localisation(rec.get("lat"), rec.get("lon"))

        # embeddings: priorité FR si dispo, sinon fallback EN
        preferences_text = ", ".join(preferences) if preferences else ""
        merged_text = " ".join(
            [x for x in [titre_fr or titre, bio_fr or bio, preferences_text] if x]
        ).strip()

        merged_vec: List[float] = []
        if merged_text:
            try:
                merged_vec = _embedding(merged_text)
            except Exception as e:
                logging.exception("Erreur embedding event_id=%s: %s", event_id, e)
                merged_vec = []

        doc: Dict[str, Any] = {
            "event_id": str(event_id) if event_id is not None else None,
            "winkerId": str(winker_id) if winker_id is not None else None,
            "boost": boost,
            "titre": titre,
            "titre_fr": titre_fr,
            "bio": bio,
            "bio_fr": bio_fr,
            "preferences": preferences,
        }

        if localisation:
            doc["localisation"] = localisation

        if merged_vec:
            doc["embedding_vector"] = merged_vec

        # _id ES = id event (upsert)
        actions.append(
            {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": str(event_id),
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
