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
  lon,
  thumbnails,
  "priceEvent"       AS price_event,
  "prixInitial"      AS prix_initial,
  "prixReduction"    AS prix_reduction,
  "containReduction" AS contain_reduction,
  price_summary,
  google_reviews
FROM profil_event;
"""

SQL_SELECT_FILES = """
SELECT
  event_id,
  array_agg(image) FILTER (WHERE image IS NOT NULL AND image <> '') AS images,
  array_agg(video) FILTER (WHERE video IS NOT NULL AND video <> '') AS videos
FROM profil_filesevent
GROUP BY event_id;
"""

# Doit matcher l'ordre exact du SELECT ci-dessus (11 colonnes)
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
    "thumbnails",
    "price_event",
    "prix_initial",
    "prix_reduction",
    "contain_reduction",
    "price_summary",
    "google_reviews",
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



def _parse_thumbnails(value: Any) -> List[str]:
    """
    thumbnails: colonne JSON Postgres contenant une liste d'URLs (strings).
    Ex: ["event_955_foo_00000.jpg", "event_955_foo_00001.jpg"]
    Retourne une list[str] (vide si invalide/vide).
    """
    if value is None:
        return []

    # Déjà une liste (Postgres a déjà désérialisé le JSON)
    if isinstance(value, list):
        return [str(x) for x in value if x]

    # String JSON à parser
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if x]
        except Exception:
            logging.warning("[WARN] thumbnails: impossible de parser le JSON: %r", s[:100])
        return []

    return []


def _parse_files(value: Any) -> List[str]:
    """
    Colonne image ou video depuis profil_filesevent.
    Peut être un tableau Postgres (array_agg), une string JSON, ou None.
    Retourne une list[str] (vide si invalide/vide).
    """
    if value is None:
        return []

    if isinstance(value, list):
        return [str(x) for x in value if x]

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if x]
        except Exception:
            logging.warning("[WARN] files: impossible de parser le JSON: %r", s[:100])
        return []

    return []


def _parse_google_reviews(value: Any) -> List[dict]:
    """
    google_reviews: colonne JSONB Postgres contenant une liste d'objets avis.
    Ex: [{"author": "...", "rating": 5, "date": "...", "text": "..."}]
    Retourne une list[dict] (vide si invalide/vide/null).
    """
    if value is None:
        return []

    # Postgres a déjà désérialisé le JSONB en list
    if isinstance(value, list):
        return [r for r in value if isinstance(r, dict)]

    # String JSON à parser (cas PostgresOperator qui renvoie du texte brut)
    if isinstance(value, str):
        s = value.strip()
        if not s or s in ("[]", ""):
            return []
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [r for r in parsed if isinstance(r, dict)]
        except Exception:
            logging.warning("[WARN] google_reviews: impossible de parser le JSON: %r", s[:100])
        return []

    return []



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

    # ── Construire le dict files: { event_id -> { images, videos } } ─────────
    files_rows = ti.xcom_pull(task_ids="fetch_files_from_postgres") or []
    files_by_event: Dict[str, Dict[str, List[str]]] = {}
    for frow in files_rows:
        # colonnes: event_id, images, videos
        if len(frow) < 3:
            continue
        eid = str(frow[0])
        files_by_event[eid] = {
            "images": _parse_files(frow[1]),
            "videos": _parse_files(frow[2]),
        }
    logging.info("Fichiers chargés pour %d events", len(files_by_event))

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

    skipped_no_id = 0
    skipped_embedding_error = 0
    skipped_localisation_error = 0

    for row in rows:
        # mapping safe: s'arrête au min des deux longueurs
        m = min(len(COL_NAMES), len(row))
        rec = {COL_NAMES[i]: row[i] for i in range(m)}

        event_id = rec.get("id")

        # ── GUARD: _id ES ne peut pas être None ──────────────────────────────
        if event_id is None:
            skipped_no_id += 1
            logging.warning("[SKIP] event_id=None -> row ignorée: %s", rec)
            continue

        winker_id = rec.get("winker_id")

        titre = (rec.get("titre") or "").strip()
        titre_fr = (rec.get("titre_fr") or "").strip()
        bio = (rec.get("bio") or "").strip()
        bio_fr = (rec.get("bio_fr") or "").strip()

        preferences = _parse_preferences(rec.get("preferences"))
        boost = _safe_int(rec.get("boost"), default=0)

        # ── Champs prix ───────────────────────────────────────────────────────
        price_event     = (rec.get("price_event") or "").strip() or None
        prix_initial    = rec.get("prix_initial")
        prix_reduction  = rec.get("prix_reduction")
        contain_reduc   = rec.get("contain_reduction")
        price_summary   = (rec.get("price_summary") or "").strip() or None

        try:
            prix_initial   = float(prix_initial)   if prix_initial   not in (None, "", "nan") else None
        except Exception:
            prix_initial   = None
        try:
            prix_reduction = float(prix_reduction) if prix_reduction not in (None, "", "nan") else None
        except Exception:
            prix_reduction = None

        is_free = (prix_initial == 0.0) or (price_event or "").lower() == "gratuit"

        if contain_reduc is not None:
            try:
                contain_reduc = str(contain_reduc).lower() in ("true", "1", "yes")
            except Exception:
                contain_reduc = False
        else:
            contain_reduc = False

        # ── Thumbnails ────────────────────────────────────────────────────────
        thumbnails = _parse_thumbnails(rec.get("thumbnails"))

        # ── Google Reviews ────────────────────────────────────────────────────
        google_reviews = _parse_google_reviews(rec.get("google_reviews"))

        # ── Images & Videos (depuis profil_filesevent) ────────────────────────
        event_files = files_by_event.get(str(event_id), {})
        images = event_files.get("images", [])
        videos = event_files.get("videos", [])

        # ── Localisation ──────────────────────────────────────────────────────
        raw_lat, raw_lon = rec.get("lat"), rec.get("lon")
        localisation = _build_localisation(raw_lat, raw_lon)
        if (raw_lat is not None or raw_lon is not None) and localisation is None:
            skipped_localisation_error += 1
            logging.warning(
                "[WARN] event_id=%s localisation invalide (lat=%r lon=%r) -> champ omis",
                event_id, raw_lat, raw_lon,
            )

        # ── Embeddings séparés (titre_vector, bio_vector, preferences_vector) ──
        preferences_text = ", ".join(preferences) if preferences else ""
        titre_text = (titre_fr or titre).strip()
        bio_text = (bio_fr or bio).strip()

        def _safe_embedding(text: str, field: str) -> List[float]:
            if not text:
                return []
            try:
                vec = _embedding(text)
                if vec and len(vec) != EMBEDDING_DIMS:
                    logging.warning(
                        "[WARN] event_id=%s field=%s embedding ignoré: dims=%s attendu=%s",
                        event_id, field, len(vec), EMBEDDING_DIMS,
                    )
                    return []
                return vec
            except Exception as e:
                logging.warning(
                    "[WARN] event_id=%s field=%s erreur embedding -> omis: %s",
                    event_id, field, e,
                )
                return []

        titre_vec = _safe_embedding(titre_text, "titre_vector")
        bio_vec = _safe_embedding(bio_text, "bio_vector")
        preferences_vec = _safe_embedding(preferences_text, "preferences_vector")

        if not any([titre_vec, bio_vec, preferences_vec]):
            skipped_embedding_error += 1
            logging.info("[INFO] event_id=%s aucun embedding produit", event_id)

        # ── Construction du document ──────────────────────────────────────────
        doc: Dict[str, Any] = {
            "event_id": str(event_id),
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

        if titre_vec:
            doc["titre_vector"] = titre_vec
        if bio_vec:
            doc["bio_vector"] = bio_vec
        if preferences_vec:
            doc["preferences_vector"] = preferences_vec

        if thumbnails:
            doc["thumbnails"] = thumbnails

        if images:
            doc["image"] = images

        if videos:
            doc["video"] = videos

        # ── Prix ──────────────────────────────────────────────────────────────
        if price_event    is not None: doc["priceEvent"]       = price_event
        if prix_initial   is not None: doc["prixInitial"]      = prix_initial
        if prix_reduction is not None: doc["prixReduction"]    = prix_reduction
        doc["containReduction"] = contain_reduc
        doc["isFree"]           = is_free
        if price_summary  is not None: doc["price_summary"]    = price_summary

        if google_reviews:
            doc["google_reviews"] = google_reviews

        logging.debug(
            "[DOC] event_id=%s titre=%r has_localisation=%s titre_vec=%s bio_vec=%s pref_vec=%s thumbnails=%d images=%d videos=%d google_reviews=%d",
            event_id, titre, localisation is not None, bool(titre_vec), bool(bio_vec), bool(preferences_vec),
            len(thumbnails), len(images), len(videos), len(google_reviews),
        )

        actions.append(
            {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": str(event_id),
                "_source": doc,
            }
        )

    logging.info(
        "📦 Préparation terminée: %d actions | skipped no_id=%d embedding_err=%d localisation_err=%d",
        len(actions), skipped_no_id, skipped_embedding_error, skipped_localisation_error,
    )

    if not actions:
        logging.warning("Aucune action à envoyer à Elasticsearch.")
        return

    # ── Bulk avec logging détaillé des erreurs (ne crash plus sur échecs partiels) ──
    success, errors = helpers.bulk(
        es,
        actions,
        raise_on_error=False,
        raise_on_exception=False,
    )

    logging.info(
        "✅ Indexation terminée: %d succès / %d total -> index=%s",
        success, len(actions), INDEX_NAME,
    )

    if errors:
        logging.warning("❌ %d document(s) en erreur lors du bulk:", len(errors))
        for err in errors:
            # err est de la forme: {"index": {"_id": "...", "error": {...}, "status": ...}}
            op = next(iter(err.values()), {})
            doc_id = op.get("_id", "?")
            status = op.get("status", "?")
            error_detail = op.get("error", {})
            error_type = error_detail.get("type", "?")
            error_reason = error_detail.get("reason", "?")
            logging.warning(
                "  → _id=%s status=%s type=%s reason=%s",
                doc_id, status, error_type, error_reason,
            )
            # Log complet pour debug approfondi
            logging.debug("  → full error: %s", json.dumps(err, ensure_ascii=False))


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

    fetch_files_from_postgres = PostgresOperator(
        task_id="fetch_files_from_postgres",
        postgres_conn_id="my_postgres",
        sql=SQL_SELECT_FILES,
        do_xcom_push=True,
    )

    index_events_to_elasticsearch = PythonOperator(
        task_id="index_events_to_elasticsearch",
        python_callable=index_events_to_es,
    )

    [fetch_events_from_postgres, fetch_files_from_postgres] >> index_events_to_elasticsearch