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
    logging.info(f"📥 Insertion d'une nouvelle ligne pour l'event {event_id}")
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

        relative_path = video_path.replace("/opt/airflow/videos/", "")

        cursor.execute("""
            INSERT INTO profil_filesevent (event_id, video, image)
            VALUES (%s, %s, NULL);
        """, (event_id, relative_path))

        connection.commit()
        logging.info("✅ Nouvelle vidéo insérée dans la base Django.")

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

def process_validated_scrapping_videos(conn_id='my_postgres'):
    logging.info("🔍 Récupération des vidéos validées dans profil_scrapping_video...")
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
            SELECT id, bio, video, addresse, site_web, site_reservation, validation,
                   code_postal, region, titre, hastags
            FROM profil_scrapping_video
            WHERE validation = 'true'
        """)
        rows = cursor.fetchall()
        logging.info(f"📦 {len(rows)} vidéos validées à traiter.")

        for row in rows:
            logging.info(f"🔍 Traitement de la ligne : {row}")
            (_id, bio, video_url, addresse, site_web, site_reservation, validation,
             code_postal, region, titre, hashtags) = row

            logging.info(f"➡️ Traitement de l’entrée ID={_id} : {titre}")

            creatorWinkerId = 116  # ID du créateur Winker( nacim.souni@outlook.fr )

            # Création de l'event
            cursor.execute("""
                INSERT INTO profil_event (titre, adresse, region, city, "codePostal", "bioEvent", website, "creatorWinker_id")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                titre, addresse, region, region, code_postal, bio, site_web, creatorWinkerId
            ))

            event_id = cursor.fetchone()[0]
            logging.info(f"🆕 Nouvel event ID={event_id} créé.")

            # Télécharger et convertir la vidéo
            final_path = download_and_prepare_tiktok_video(video_url)
            relative_path = final_path.replace("/opt/airflow/videos/", "")

            # Insertion de la vidéo liée
            cursor.execute("""
                INSERT INTO profil_filesevent (event_id, video, image)
                VALUES (%s, %s, NULL)
            """, (event_id, relative_path))
            logging.info(f"🎥 Vidéo insérée pour l'event ID={event_id}")

        connection.commit()
        logging.info("✅ Toutes les vidéos ont été traitées.")

    except Exception as e:
        logging.error(f"❌ Erreur : {e}")
        raise
    finally:
        cursor.close()
        connection.close()


with DAG(
    dag_id="download_tiktok_video_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tiktok", "video", "download"],
) as dag:

    download_task = PythonOperator(
        task_id="process_scrapping_videos",
        python_callable=process_validated_scrapping_videos,
    )
