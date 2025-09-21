from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import logging
import shutil
import yt_dlp  # via requirements.txt (conseillé: yt-dlp==2025.09.05)

PARIS_TZ = ZoneInfo("Europe/Paris")

# Dossier de sortie final
OUTPUT_DIR = os.environ.get("YOUTUBE_OUTPUT_DIR", "/opt/airflow/videos")

# URL du Short (forme canonique pour éviter certains blocages)
YOUTUBE_SHORT_FR = "https://www.youtube.com/shorts/s2eDWQCBieE"



def _have_ffmpeg():
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None


def download_youtube_short(url: str, filename: str):
    """
    Télécharge un Short YouTube en MP4 (H.264 + AAC) avec plusieurs stratégies.
    Écrit le fichier final dans OUTPUT_DIR/filename
    """
    if not _have_ffmpeg():
        raise RuntimeError("ffmpeg/ffprobe introuvables dans le conteneur Airflow.")

    final_path = os.path.join(OUTPUT_DIR, filename)
    tmp_path = os.path.join(OUTPUT_DIR, filename + ".part.mp4")

    # En-têtes « navigateur » pour limiter les 403
    headers = {
        "User-Agent": os.environ.get(
            "YTDLP_UA",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": os.environ.get("YTDLP_ACCEPT_LANGUAGE", "en-US,en;q=0.7"),
    }

    # Option cookies facultative (si besoin de consentement)
    cookies_file = os.environ.get("YTDLP_COOKIES")  # ex: /opt/airflow/cookies.txt
    if cookies_file and not os.path.exists(cookies_file):
        logging.warning(f"cookies.txt indiqué mais introuvable: {cookies_file}")
        cookies_file = None

    # Préférence formats: vidéo AVC (H.264) + audio m4a → MP4
    base_opts = {
        "noplaylist": True,
        "quiet": True,
        "retries": 10,
        "fragment_retries": 10,
        "http_headers": headers,
        "outtmpl": tmp_path,                  # d'abord en temporaire
        "merge_output_format": "mp4",         # merge en MP4 si possible
        "format": "bv*[vcodec^=avc1]+ba[ext=m4a]/b[ext=mp4]/b",
        "overwrites": True,
    }
    if cookies_file:
        base_opts["cookiefile"] = cookies_file

    # On tente plusieurs clients YouTube si « web » échoue
    clients = ["web", "ios", "tv"]
    last_err = None

    for client in clients:
        opts = dict(base_opts)
        opts["extractor_args"] = {"youtube": {"player_client": [client]}}
        logging.info(f"⬇️  Tentative yt-dlp client={client} → {filename}")
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])

            # Renommage du tmp en final (selon versions yt-dlp)
            if os.path.exists(tmp_path):
                os.replace(tmp_path, final_path)
            elif os.path.exists(final_path):
                pass
            else:
                # Cherche un .mp4 plausible écrit à côté
                candidate = None
                for f in os.listdir(OUTPUT_DIR):
                    fp = os.path.join(OUTPUT_DIR, f)
                    if f.endswith(".mp4") and os.path.getsize(fp) > 0 and f.startswith(os.path.splitext(filename)[0]):
                        candidate = fp
                        break
                if candidate:
                    os.replace(candidate, final_path)
                else:
                    raise FileNotFoundError("Fichier MP4 attendu introuvable après téléchargement.")

            logging.info(f"✅ Terminé: {final_path}")
            return final_path

        except Exception as e:
            last_err = e
            logging.warning(f"⚠️  Échec client={client}: {e}")

    # Fallback : ré-encodage explicite en MP4
    logging.info("🛠️  Fallback: ré-encodage en MP4 via FFmpeg")
    pp_opts = dict(base_opts)
    pp_opts["postprocessors"] = [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}]

    try:
        with yt_dlp.YoutubeDL(pp_opts) as ydl:
            ydl.download([url])

        if os.path.exists(tmp_path):
            os.replace(tmp_path, final_path)
        elif os.path.exists(final_path):
            pass
        else:
            candidate = None
            for f in os.listdir(OUTPUT_DIR):
                fp = os.path.join(OUTPUT_DIR, f)
                if f.endswith(".mp4") and os.path.getsize(fp) > 0 and f.startswith(os.path.splitext(filename)[0]):
                    candidate = fp
                    break
            if candidate:
                os.replace(candidate, final_path)
            else:
                raise FileNotFoundError("Fichier MP4 attendu introuvable après fallback.")

        logging.info(f"✅ Terminé (fallback): {final_path}")
        return final_path

    except Exception as e:
        raise RuntimeError(f"❌ Impossible de télécharger {url} → {filename}. Dernière erreur: {last_err} / Fallback: {e}")


def task_download_fr():
    return download_youtube_short(YOUTUBE_SHORT_FR, "video_explicative_en.mp4")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="download_youtube_short_fr",
    description="Télécharge le Short YouTube FR en MP4 dans /opt/airflow/videos",
    start_date=datetime(2024, 1, 1, tzinfo=PARIS_TZ),
    schedule_interval=None,   # lancement manuel
    catchup=False,
    default_args=default_args,
    tags=["youtube", "short", "video"],
) as dag:

    dag.timezone = PARIS_TZ

    download_fr = PythonOperator(
        task_id="download_video_explicative_en",
        python_callable=task_download_fr,
    )
