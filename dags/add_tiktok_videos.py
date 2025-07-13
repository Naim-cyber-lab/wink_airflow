from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import uuid
import subprocess
import yt_dlp
import logging
import psycopg2


def check_raw_video_codec(path):
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    codec = result.stdout.strip()
    logging.info(f"🎥 Codec de la vidéo brute ({path}) : {codec}")
    return codec


def download_and_prepare_tiktok_video(url, output_dir="/opt/airflow/videos"):
    os.makedirs(output_dir, exist_ok=True)
    video_id = str(uuid.uuid4())
    raw_path = os.path.join(output_dir, f"{video_id}_raw.mp4")
    final_path = os.path.join(output_dir, f"{video_id}.mp4")

    logging.info("⏬ Téléchargement de la vidéo TikTok...")
    ydl_opts = {'outtmpl': raw_path, 'format': 'best', 'quiet': True}

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        logging.info("✅ Téléchargement terminé.")
    except Exception as e:
        raise RuntimeError(f"❌ Erreur pendant le téléchargement : {e}")

    codec = check_raw_video_codec(raw_path)
    if codec != "hevc":
        logging.warning(f"⚠️ Codec inattendu : {codec} (attendu : hevc)")

    logging.info("🎞️ Conversion en H.264 (AVC) + AAC...")
    ffmpeg_cmd = [
        "ffmpeg", "-y", "-i", raw_path,
        "-c:v", "libx264", "-profile:v", "baseline", "-level", "3.0", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart",
        final_path
    ]

    result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error("❌ Erreur ffmpeg :\n" + result.stderr)
        raise RuntimeError("Conversion échouée.")
    logging.info(f"✅ Conversion réussie : {final_path}")

    if os.path.exists(raw_path):
        os.remove(raw_path)
        logging.info(f"🧹 Fichier brut supprimé : {raw_path}")

    return final_path


def insert_video_into_django_db(video_path, event_id=1, conn_id='my_postgres'):
    logging.info(f"📥 Insertion vidéo pour l'event {event_id}")
    conn = BaseHook.get_connection(conn_id)

    try:
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )
        cursor = connection.cursor()

        cursor.execute("""
            SELECT id FROM profil_filesevent
            WHERE event_id = %s AND image IS NULL
            LIMIT 1;
        """, (event_id,))
        row = cursor.fetchone()

        if row:
            file_event_id = row[0]
            logging.info(f"📝 Mise à jour de FilesEvent ID={file_event_id}")
            relative_path = video_path.replace("/opt/airflow/videos/", "videos/")

            cursor.execute("""
                UPDATE profil_filesevent
                SET video = %s
                WHERE id = %s;
            """, (relative_path, file_event_id))

            connection.commit()
            logging.info("✅ Vidéo insérée dans la base Django.")
        else:
            logging.warning("⚠️ Aucun FilesEvent trouvé avec image NULL.")

    except Exception as e:
        logging.error(f"❌ Erreur PostgreSQL : {e}")
        raise
    finally:
        cursor.close()
        connection.close()


def run_tiktok_download():
    url = "https://www.tiktok.com/@sofiaosaoncamara/video/7352841442324221217"
    final_path = download_and_prepare_tiktok_video(url)
    logging.info(f"🎯 Vidéo téléchargée : {final_path}")
    insert_video_into_django_db(final_path, event_id=1)


with DAG(
    dag_id="download_tiktok_video_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tiktok", "video", "download"],
) as dag:

    download_task = PythonOperator(
        task_id="download_and_prepare_video",
        python_callable=run_tiktok_download,
    )
