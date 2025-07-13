from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import uuid
import subprocess
import yt_dlp
import logging


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
    """
    Télécharge une vidéo TikTok, la convertit en H.264 (AVC) + AAC pour React Native,
    et retourne le chemin final de la vidéo.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Noms de fichiers temporaires
    video_id = str(uuid.uuid4())
    raw_path = os.path.join(output_dir, f"{video_id}_raw.mp4")
    final_path = os.path.join(output_dir, f"{video_id}.mp4")

    logging.info("⏬ Téléchargement de la vidéo TikTok...")
    ydl_opts = {
        'outtmpl': raw_path,
        'format': 'best',
        'quiet': True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        logging.info("✅ Téléchargement terminé.")
    except Exception as e:
        raise RuntimeError(f"❌ Erreur pendant le téléchargement : {e}")

    codec = check_raw_video_codec(raw_path)
    if codec != "hevc":
        logging.warning(f"⚠️ Codec inattendu : {codec} (attendu : hevc)")

    logging.info("🎞️ Conversion en format H.264 (AVC) + AAC...")
    ffmpeg_cmd = [
        "ffmpeg",
        "-y",  # overwrite
        "-i", raw_path,
        "-c:v", "libx264",
        "-profile:v", "baseline",
        "-level", "3.0",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        final_path
    ]

    try:
        result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logging.error("❌ Erreur ffmpeg :\n" + result.stderr)
            raise RuntimeError("Conversion échouée.")
        logging.info(f"✅ Conversion réussie. Fichier prêt à l'emploi : {final_path}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"❌ Erreur pendant la conversion avec ffmpeg : {e}")

    # Nettoyer le fichier source brut
    if os.path.exists(raw_path):
        os.remove(raw_path)
        logging.info(f"🧹 Fichier brut supprimé : {raw_path}")

    return final_path


def run_tiktok_download():
    url = "https://www.tiktok.com/@sofiaosaoncamara/video/7352841442324221217"
    result = download_and_prepare_tiktok_video(url)
    logging.info(f"🎯 Vidéo prête : {result}")


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
