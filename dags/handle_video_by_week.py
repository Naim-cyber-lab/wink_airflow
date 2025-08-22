from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import psycopg2
import logging

# --- DOC Airflow: nullify_publication_last_week ------------------------------
DAG_DOC = r"""
# 🧹 Nettoyage des publications anciennes

**But**  
Désactiver (`active = 0`) les événements dont la date de publication est **strictement antérieure à J-1**.

**Planification**  
- CRON : `59 23 * * 0` (tous les dimanches à 23:59)  
- `catchup = False`

**Source & Connexion**  
- Connexion Airflow : **my_postgres**  
- Table : **profil_event** (`datePublication`, `active`)

**Logique**  
- Exécute la requête ci-dessous et logge le nombre de lignes affectées.

```sql
UPDATE profil_event
SET "active" = 0
WHERE "datePublication" < CURRENT_DATE - INTERVAL '1 day';
"""

DB_CONN_ID = 'my_postgres'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def nullify_old_publications(**kwargs):
    execution_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")  # date d'exécution du DAG
    start_date = (execution_date - timedelta(days=14)).date()

    logging.info(f"🔍 Mise à null des datePublication inférieur à {start_date}")

    try:
        conn = BaseHook.get_connection(DB_CONN_ID)
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )

        cursor = connection.cursor()
        cursor.execute("""
            UPDATE profil_event
            SET "active" = 0
            WHERE "datePublication" < CURRENT_DATE - INTERVAL '1 day';
        """)

        affected = cursor.rowcount
        connection.commit()
        logging.info(f"✅ {affected} événements mis à jour.")
        cursor.close()
        connection.close()

    except Exception as e:
        logging.error(f"❌ Erreur dans nullify_old_publications : {e}")
        raise

with DAG(
    dag_id='nullify_publication_last_week',
    description='Met à NULL les datePublication de la semaine passée (J-14 à J-7)',
    schedule_interval='59 23 * * 0',  # chaque dimanche à 23h59
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['cleanup', 'event', 'profil_event'],
) as dag:

    dag.doc_md = DAG_DOC

    nullify_task = PythonOperator(
        task_id='nullify_old_publications',
        python_callable=nullify_old_publications,
        provide_context=True,
    )
