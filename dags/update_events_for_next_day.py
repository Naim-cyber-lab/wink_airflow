from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import psycopg2
import logging

DB_CONN_ID = 'my_postgres'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def get_target_per_region(**kwargs):
    ti = kwargs['ti']
    logging.info("🎯 [get_target_per_region] Début de la récupération des objectifs de publication par région.")
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
            SELECT config_one, float_param
            FROM profil_nisu_param_config
            WHERE perimeter = 'video'
        """)
        targets = {row[0]: int(row[1]) for row in cursor.fetchall()}
        logging.info(f"✅ Objectifs extraits de la table profil_nisu_param_config : {targets}")
        cursor.close()
        connection.close()
        ti.xcom_push(key='targets', value=targets)
    except Exception as e:
        logging.error(f"❌ Erreur dans get_target_per_region : {e}")
        raise

def get_published_events(**kwargs):
    from datetime import datetime, timedelta
    import logging
    import psycopg2
    from airflow.hooks.base import BaseHook

    ti = kwargs['ti']
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    logging.info(f"📦 [get_published_events] Vérification des événements avec datePublication = {tomorrow.isoformat()}")

    try:
        conn = BaseHook.get_connection(DB_CONN_ID)
        with psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        ) as connection:
            with connection.cursor() as cursor:
                # Active tout ce qui est prévu pour demain
                cursor.execute("""
                    UPDATE profil_event
                    SET active = 1
                    WHERE "datePublication" = %s
                """, (tomorrow,))
                connection.commit()

                # Compte par région, en forçant la présence de toutes les régions cibles
                cursor.execute("""
                    SELECT t.config_one AS region, COALESCE(COUNT(e.*), 0) AS cnt
                    FROM profil_nisu_param_config t
                    LEFT JOIN profil_event e
                      ON e.region = t.config_one
                     AND e."datePublication" = %s
                     AND e.active = 1
                    WHERE t.perimeter = 'video'
                    GROUP BY t.config_one
                    ORDER BY t.config_one
                """, (tomorrow,))

                published = {row[0]: int(row[1]) for row in cursor.fetchall()}

                logging.info(f"✅ Événements déjà publiés demain par région (avec zéros) : {published}")
                ti.xcom_push(key='published', value=published)

    except Exception as e:
        logging.error(f"❌ Erreur dans get_published_events : {e}")
        raise

def compute_missing_events(**kwargs):
    ti = kwargs['ti']
    logging.info("🧠 [compute_missing_events] Début du calcul des événements manquants par région.")
    try:
        targets = ti.xcom_pull(task_ids='get_target_per_region', key='targets')
        published = ti.xcom_pull(task_ids='get_published_events', key='published')
        logging.info(f"📥 Objectifs (targets) récupérés : {targets}")
        logging.info(f"📥 Publiés (published) récupérés : {published}")

        missing = {}
        for region, target in targets.items():
            current = published.get(region, 0)
            to_add = max(target - current, 0)
            if to_add > 0:
                missing[region] = to_add
                logging.info(f"🔍 Région '{region}' : Objectif={target}, Actuel={current}, Manquant={to_add}")
            else:
                logging.info(f"✅ Région '{region}' : Objectif atteint ou dépassé ({current}/{target})")

        if not missing:
            logging.info("📗 Aucun événement à compléter. Toutes les régions ont atteint leur quota.")
        else:
            logging.info(f"📉 Événements manquants à ajouter : {missing}")

        ti.xcom_push(key='missing', value=missing)

    except Exception as e:
        logging.error(f"❌ Erreur dans compute_missing_events : {e}")
        raise

def update_events(**kwargs):
    ti = kwargs['ti']
    logging.info("🛠️ [update_events] Début de la mise à jour des événements manquants.")
    try:
        missing = ti.xcom_pull(task_ids='compute_missing_events', key='missing')
        tomorrow = (datetime.now() + timedelta(days=1)).date()
        logging.info(f"📆 Date cible pour publication : {tomorrow}")
        logging.info(f"🧮 Données à mettre à jour : {missing}")

        conn = BaseHook.get_connection(DB_CONN_ID)
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )
        cursor = connection.cursor()

        total_updated = 0

        for region, to_add in missing.items():
            logging.info(f"📂 Traitement de la région : {region} (événements à ajouter : {to_add})")

            cursor.execute("""
                    SELECT id
                    FROM profil_event
                    WHERE "dateEvent" IS NULL
                      AND region = %s
                      AND (active = 0 OR active IS NULL)
                    ORDER BY "datePublication" IS NOT NULL, "datePublication" ASC
                    LIMIT %s;
            """, (region, to_add))

            candidates = cursor.fetchall()
            logging.info(f"🔎 {len(candidates)} événements anciens disponibles pour {region}.")

            for (event_id,) in candidates:
                cursor.execute("""
                    UPDATE profil_event
                    SET "datePublication" = %s,  active = 1
                    WHERE id = %s
                """, (tomorrow, event_id))
                logging.info(f"✅ Event ID {event_id} modifié pour publication le {tomorrow}")
                total_updated += 1

        connection.commit()
        logging.info(f"🎉 Mise à jour terminée : {total_updated} événements replanifiés.")

        cursor.close()
        connection.close()

    except Exception as e:
        logging.error(f"❌ Erreur dans update_events : {e}")
        raise

with DAG(
    dag_id="update_events_for_next_day_xcom",
    start_date=days_ago(1),
    schedule_interval="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["event", "region", "xcom", "log"],
) as dag:

    t1 = PythonOperator(
        task_id="get_target_per_region",
        python_callable=get_target_per_region,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="get_published_events",
        python_callable=get_published_events,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="compute_missing_events",
        python_callable=compute_missing_events,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="update_events",
        python_callable=update_events,
        provide_context=True,
    )

    [t1, t2] >> t3 >> t4
