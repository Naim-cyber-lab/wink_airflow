from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import logging
import psycopg2

# 📁 Répertoire contenant les vidéos
VIDEOS_PATH = "/opt/airflow/videos"
DB_CONN_ID = "my_postgres"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def clean_orphan_videos():
    logging.info("🚀 Début de la tâche : suppression des vidéos orphelines.")

    # Connexion DB via Airflow
    logging.info(f"🔌 Connexion à la base via la connexion Airflow : {DB_CONN_ID}")
    conn = BaseHook.get_connection(DB_CONN_ID)

    try:
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )
        cursor = connection.cursor()

        # Récupération des chemins de fichiers vidéo en base
        logging.info("📥 Récupération des fichiers présents dans la table profil_filesevent...")
        cursor.execute("SELECT video FROM profil_filesevent;")
        rows = cursor.fetchall()
        db_files = set(row[0] for row in rows)
        logging.info(f"✅ {len(db_files)} fichiers trouvés en base.")

        # Fichiers réellement présents dans le volume
        logging.info(f"📂 Lecture du répertoire : {VIDEOS_PATH}")
        local_files = set(os.listdir(VIDEOS_PATH))
        logging.info(f"📁 {len(local_files)} fichiers trouvés localement.")

        # Détection des fichiers orphelins
        orphan_files = local_files - db_files
        logging.info(f"🕵️‍♂️ {len(orphan_files)} fichiers orphelins détectés : {orphan_files}")

        # Suppression
        for file in orphan_files:
            file_path = os.path.join(VIDEOS_PATH, file)
            try:
                #os.remove(file_path)
                logging.info(f"🧹 Fichier supprimé : {file}")
            except Exception as e:
                logging.error(f"❌ Erreur en supprimant {file} : {e}")

        logging.info("✅ Tâche terminée avec succès.")

    except Exception as e:
        logging.error(f"❌ Erreur globale dans le DAG : {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

with DAG(
    dag_id="clean_orphan_videos_dag",
    description="Supprime les vidéos du volume qui ne sont pas référencées en base",
    schedule_interval="0 3 * * *",  # Tous les jours à 3h du matin
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cleanup", "videos", "maintenance"],
    default_args=default_args,
) as dag:

    clean_task = PythonOperator(
        task_id="delete_orphan_videos",
        python_callable=clean_orphan_videos,
    )
