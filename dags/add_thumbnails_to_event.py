import os
import json
import logging
import tempfile
from datetime import timedelta
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import cv2
import psycopg2
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ================== CONFIG ==================
PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

# Dans docker-compose: ./videos -> /opt/airflow/videos
VIDEOS_DIR = Variable.get("THUMBS_OUTPUT_DIR", default_var="/opt/airflow/videos").strip()

# Préfixe à ajouter devant les noms .mp4 venant de la BDD
MEDIAFILES_BASE_URL = Variable.get(
    "MEDIAFILES_BASE_URL",
    default_var="https://api.nisu.fr/mediafiles/",
).strip()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ================== DB HELPERS ==================
def _pg_connect():
    """Connexion PostgreSQL via la Connection Airflow `my_postgres`."""
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


# ================== VIDEO/THUMB HELPERS ==================
def is_url(path: str) -> bool:
    p = urlparse(path)
    return p.scheme in ("http", "https") and bool(p.netloc)


def download_file(url: str, dst_path: str, chunk_size: int = 1024 * 1024) -> str:
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    with open(dst_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
    return dst_path


def build_video_url_or_path(video_value: str) -> str:
    """
    Dans ta BDD (profil_filesevent.video), tu as souvent juste un nom de fichier .mp4.
    Règle demandée:
      - Si c'est une URL -> on la retourne
      - Sinon -> on préfixe avec https://api.nisu.fr/mediafiles/
    """
    if not video_value:
        return ""

    video_value = str(video_value).strip()

    if is_url(video_value):
        return video_value

    # Assure un seul slash
    base = MEDIAFILES_BASE_URL.rstrip("/") + "/"
    filename = video_value.lstrip("/")
    return base + filename


def _safe_stem(name: str) -> str:
    """Nettoie un nom de fichier (sans extension) pour éviter caractères bizarres."""
    keep = []
    for ch in name:
        if ch.isalnum() or ch in ("-", "_"):
            keep.append(ch)
        else:
            keep.append("_")
    return "".join(keep).strip("_") or "file"


def extract_thumbnails(
    video_input: str,
    out_dir: str,
    total_thumbs: int = 2,  # <-- EXACTEMENT 2 images par vidéo
    image_format: str = "jpg",
    prefix: str = "thumb",
    jpeg_quality: int = 90,
) -> list[str]:
    """
    Extrait EXACTEMENT `total_thumbs` thumbnails répartis sur la vidéo.
    - Si video_input est une URL, la vidéo est téléchargée en temporaire puis lue par OpenCV.
    Retourne les chemins ABSOLUS des images générées.
    """
    out_dir_abs = os.path.abspath(out_dir)
    os.makedirs(out_dir_abs, exist_ok=True)

    temp_video_path = None
    video_path = video_input

    # URL -> téléchargement temp
    if is_url(video_input):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        tmp.close()
        temp_video_path = tmp.name
        download_file(video_input, temp_video_path)
        video_path = temp_video_path

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        if temp_video_path and os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        raise RuntimeError(f"Impossible d'ouvrir la vidéo: {video_input}")

    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    if frame_count <= 0:
        cap.release()
        if temp_video_path and os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        raise RuntimeError(f"Frame count inconnu pour: {video_input}")

    total_thumbs = max(1, int(total_thumbs))

    # Indices répartis sur la vidéo (20% et 80% si 2 thumbs)
    if total_thumbs == 1:
        frame_indices = [frame_count // 2]
    elif total_thumbs == 2:
        frame_indices = [
            int(0.2 * (frame_count - 1)),
            int(0.8 * (frame_count - 1)),
        ]
    else:
        frame_indices = []
        for i in range(total_thumbs):
            t = i / (total_thumbs - 1)  # 0..1
            idx = int(round(t * (frame_count - 1)))
            frame_indices.append(idx)

    written: list[str] = []
    encode_params = []
    if image_format.lower() in ("jpg", "jpeg"):
        encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), int(jpeg_quality)]

    for i, idx in enumerate(frame_indices):
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        if not ok or frame is None:
            continue

        out_path = os.path.join(out_dir_abs, f"{prefix}_{i:05d}.{image_format}")
        if cv2.imwrite(out_path, frame, encode_params):
            written.append(out_path)

    cap.release()

    if temp_video_path and os.path.exists(temp_video_path):
        os.remove(temp_video_path)

    return written


def generate_thumbnails_for_event(event_id: int, video_value: str) -> list[str]:
    """
    Génère EXACTEMENT 2 thumbnails et renvoie une liste de NOMS de fichiers.
    IMPORTANT: écrit DIRECTEMENT dans /opt/airflow/videos (sans sous-dossier).
    Les noms sont uniques pour éviter collisions.
    """
    video_src = build_video_url_or_path(video_value)
    if not video_src:
        return []

    os.makedirs(VIDEOS_DIR, exist_ok=True)

    base = os.path.basename(str(video_value))  # ex: abc123.mp4
    stem = _safe_stem(os.path.splitext(base)[0])

    # Exemple: event_517_abc123_00000.jpg et event_517_abc123_00001.jpg
    prefix = f"event_{event_id}_{stem}"

    thumbs_paths = extract_thumbnails(
        video_input=video_src,
        out_dir=VIDEOS_DIR,  # <-- DIRECTEMENT dans videos/
        total_thumbs=int(Variable.get("THUMBS_PER_VIDEO", default_var="2")),
        image_format=Variable.get("THUMBS_FORMAT", default_var="jpg"),
        prefix=prefix,
        jpeg_quality=int(Variable.get("THUMBS_JPEG_QUALITY", default_var="90")),
    )

    return [os.path.basename(p) for p in thumbs_paths]


# ================== DB QUERIES ==================
def fetch_events_missing_thumbnails(limit: int = 200) -> list[tuple[int, str]]:
    """
    - profil_event.thumbnails (text)
    - profil_filesevent.video (nom fichier mp4 ou URL), liée par event_id
    """
    sql = """
        SELECT e.id AS event_id, f.video AS video
        FROM profil_event e
        JOIN profil_filesevent f ON f.event_id = e.id
        WHERE e.thumbnails IS NULL OR e.thumbnails = ''
          AND f.video IS NOT NULL
          AND f.video <> ''
        ORDER BY e.id ASC
        LIMIT %s
    """
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
            return [(int(r[0]), str(r[1])) for r in rows]
    finally:
        conn.close()


def update_event_thumbnails(event_id: int, thumbnails: list[str]) -> None:
    """Met à jour profil_event.thumbnails (text) avec un JSON texte (liste de noms de fichiers)."""
    sql = "UPDATE profil_event SET thumbnails = %s WHERE id = %s"
    payload = json.dumps(thumbnails, ensure_ascii=False)

    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (payload, event_id))
        conn.commit()
    finally:
        conn.close()


# ================== AIRFLOW TASK ==================
def add_thumbnails_to_event(**_context):
    """
    - récupère les events sans thumbnails
    - génère 2 thumbnails depuis l'URL https://api.nisu.fr/mediafiles/<nom_video>
    - écrit les thumbnails DIRECTEMENT dans /opt/airflow/videos
    - met à jour profil_event.thumbnails (JSON texte)
    """
    limit = int(Variable.get("THUMBS_BATCH_LIMIT", default_var="200"))

    rows = fetch_events_missing_thumbnails(limit=limit)
    if not rows:
        logging.info("Aucun événement à traiter (thumbnails déjà présents).")
        return

    logging.info("Événements à traiter: %s", len(rows))
    logging.info("Dossier de sortie thumbnails: %s", VIDEOS_DIR)
    logging.info("MEDIAFILES_BASE_URL: %s", MEDIAFILES_BASE_URL)

    processed = 0
    skipped = 0
    failed = 0

    for event_id, video_value in rows:
        try:
            thumbs = generate_thumbnails_for_event(event_id, video_value)
            if not thumbs:
                skipped += 1
                logging.warning("Event %s: aucune thumbnail générée (video=%s)", event_id, video_value)
                continue

            update_event_thumbnails(event_id, thumbs)
            processed += 1
            logging.info("Event %s: %s thumbnails -> DB OK (%s)", event_id, len(thumbs), thumbs)

        except Exception as e:
            failed += 1
            logging.exception("Event %s: échec génération/MAJ thumbnails (%s)", event_id, e)

    logging.info("Terminé. processed=%s skipped=%s failed=%s", processed, skipped, failed)


# ================== DAG ==================
with DAG(
    dag_id="profil_event_generate_thumbnails",
    description="Génère 2 thumbnails par vidéo (URL api.nisu.fr/mediafiles/) et les écrit directement dans /opt/airflow/videos",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    schedule_interval=None,  # mets "@hourly" si tu veux le lancer automatiquement
    tags=["event", "video", "thumbnails"],
) as dag:
    dag.timezone = PARIS_TZ

    generate_and_store_thumbnails = PythonOperator(
        task_id="generate_and_store_thumbnails",
        python_callable=add_thumbnails_to_event,
    )

    generate_and_store_thumbnails