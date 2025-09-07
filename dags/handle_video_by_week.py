# handle_video_by_week.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import timedelta, date
import pendulum
import psycopg2
import logging
from airflow.utils.dates import days_ago

# --- DOC Airflow: nullify_publication_last_week ------------------------------
DAG_DOC = r"""
# 🧹 Nettoyage des publications anciennes

**But**  
Désactiver (`active = 0`) les événements dont la date de publication est **strictement antérieure à J-7**.

**Planification**  
- CRON : `50 23 * * 0` (tous les dimanches à 23:50)  
- `catchup = False`

**Source & Connexion**  
- Connexion Airflow : **my_postgres**  
- Table : **profil_event** (`datePublication`, `active`)

**Logique**  
- Exécute la requête ci-dessous et log le nombre de lignes affectées.

```sql
UPDATE profil_event
SET "active" = 0
WHERE "datePublication" < CURRENT_DATE - INTERVAL '7 day';
```
"""

DB_CONN_ID = 'my_postgres'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def nullify_old_publications(**kwargs):
    execution_date = kwargs['logical_date']  # objet pendulum datetime
    logging.info(f"🔍 DAG exécuté à {execution_date}")

    try:
        conn = BaseHook.get_connection(DB_CONN_ID)
        with psycopg2.connect(
                host=conn.host,
                port=conn.port,
                user=conn.login,
                password=conn.password,
                dbname=conn.schema,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE profil_event
                    SET "active" = 0
                    WHERE "datePublication" < CURRENT_DATE - INTERVAL '7 day';
                    """
                )
                affected = cursor.rowcount
                connection.commit()
                logging.info(f"✅ {affected} événements mis à jour.")
    except Exception as e:
        logging.error(f"❌ Erreur dans nullify_old_publications : {e}")
        raise


with DAG(
    dag_id='nullify_publication_last_week',
    description='Met à NULL les datePublication de la semaine passée ( à J-7 )',
    schedule_interval='50 23 * * 0',  # chaque dimanche à 23h50
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['cleanup', 'event', 'profil_event'],
    timezone="Europe/Paris",
) as dag:
    dag.doc_md = DAG_DOC

    nullify_task = PythonOperator(
        task_id="nullify_old_publications",
        python_callable=nullify_old_publications,
    )

