from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import uuid
import subprocess
import yt_dlp


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

    print("⏬ Téléchargement de la vidéo TikTok...")
    ydl_opts = {
        'outtmpl': raw_path,
        'format': 'best',
        'quiet': True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        print("✅ Téléchargement terminé.")
    except Exception as e:
        raise RuntimeError(f"Erreur pendant le téléchargement : {e}")

    print("🎞️ Conversion en format H.264 (AVC) + AAC...")
    ffmpeg_cmd = [
        "ffmpeg",
        "-i", raw_path,
        "-c:v", "libx264",
        "-c:a", "aac",
        "-strict", "experimental",
        "-movflags", "+faststart",  # utile pour lecture en streaming
        final_path
    ]

    try:
        subprocess.run(ffmpeg_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"✅ Conversion réussie. Fichier prêt à l'emploi : {final_path}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Erreur pendant la conversion avec ffmpeg : {e}")

    # Nettoyer le fichier source brut
    if os.path.exists(raw_path):
        os.remove(raw_path)

    return final_path


def run_tiktok_download():
    url = "https://www.tiktok.com/@sofiaosaoncamara/video/7352841442324221217"
    result = download_and_prepare_tiktok_video(url)
    print(f"🎯 Vidéo prête : {result}")


with DAG(
    dag_id="download_tiktok_video_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # ou '0 0 * * *' pour tous les jours à minuit
    catchup=False,
    tags=["tiktok", "video", "download"],
) as dag:

    download_task = PythonOperator(
        task_id="download_and_prepare_video",
        python_callable=run_tiktok_download,
    )
