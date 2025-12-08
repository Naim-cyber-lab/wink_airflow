from datetime import datetime, date
from zoneinfo import ZoneInfo
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from elasticsearch import helpers


PARIS_TZ = ZoneInfo("Europe/Paris")
DAG_ID = "sync_winker_to_elasticsearch"
INDEX_NAME = "winker"  # ton index ES

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# ⚠️ IMPORTANT :
# Les colonnes en camelCase dans Django sont en réalité
# créées avec des guillemets en SQL → il faut les citer:
# "birthYear", "codePostal", "photoProfil"
SQL_SELECT_WINKERS = """
SELECT
    id,
    username,
    bio,
    "birthYear",
    city,
    "codePostal",
    pays,
    "photoProfil",
    region,
    sexe,
    lat,
    lon,
    last_connection
FROM profil_winker
WHERE is_active = TRUE;
"""


def create_index_if_needed(es):
    """
    Crée l'index `winker` avec TON mapping s'il n'existe pas encore.
    """
    if es.indices.exists(index=INDEX_NAME):
        logging.info("Index '%s' existe déjà, on ne le recrée pas.", INDEX_NAME)
        return

    logging.info("Index '%s' n'existe pas, création avec le mapping fourni...", INDEX_NAME)

    body = {
        "mappings": {
            "properties": {
                "age": {"type": "integer"},
                "bio": {"type": "text", "analyzer": "text_fr"},
                "birthYear": {"type": "integer"},
                "city": {
                    "type": "text",
                    "analyzer": "text_fr",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    },
                },
                "codePostal": {"type": "keyword"},
                "fun_free_text": {"type": "text", "analyzer": "text_fr"},
                "fun_options": {"type": "keyword"},
                "fun_questions": {"type": "keyword"},
                "fun_sections": {"type": "keyword"},
                "id": {"type": "integer"},
                "last_connection": {"type": "date"},
                "location": {"type": "geo_point"},
                "pays": {"type": "keyword"},
                "photoProfil": {"type": "keyword"},
                "preferences": {"type": "keyword"},
                "preferences_raw": {"type": "text"},
                "region": {"type": "keyword"},
                "sexe": {"type": "keyword"},
                "username": {
                    "type": "text",
                    "analyzer": "text_fr",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256,
                        }
                    },
                },
            }
        }
    }

    es.indices.create(index=INDEX_NAME, body=body)
    logging.info("Index '%s' créé.", INDEX_NAME)


def index_winkers_to_es(ti, **_):
    """
    Récupère les winkers depuis Postgres (via XCom) et les indexe dans ES.
    Aucun doublon : on utilise _id = id du Winker.
    """
    rows = ti.xcom_pull(task_ids="fetch_winkers_from_postgres") or []
    if not rows:
        logging.info("Aucun winker retourné par la requête SQL.")
        return

    col_names = [
        "id",
        "username",
        "bio",
        "birthYear",
        "city",
        "codePostal",
        "pays",
        "photoProfil",
        "region",
        "sexe",
        "lat",
        "lon",
        "last_connection",
    ]

    # Connexion ES via la connexion Airflow (à créer dans l'UI : elasticsearch_default)
    hook = ElasticsearchHook(elasticsearch_conn_id="elasticsearch_default")
    es = hook.get_conn()

    # Création index si besoin
    create_index_if_needed(es)

    current_year = date.today().year
    actions = []

    for row in rows:
        record = {col_names[i]: row[i] for i in range(len(col_names))}

        winker_id = record["id"]
        birth_year = record.get("birthYear")
        age = None
        if birth_year:
            try:
                age = current_year - int(birth_year)
            except Exception:
                age = None

        lat = record.get("lat")
        lon = record.get("lon")
        location = None
        if lat is not None and lon is not None:
            try:
                location = {"lat": float(lat), "lon": float(lon)}
            except Exception:
                location = None

        es_doc = {
            "id": winker_id,
            "username": record.get("username"),
            "bio": record.get("bio"),
            "birthYear": birth_year,
            "age": age,
            "city": record.get("city"),
            "codePostal": record.get("codePostal"),
            "pays": record.get("pays"),
            "photoProfil": str(record.get("photoProfil")) if record.get("photoProfil") else None,
            "region": record.get("region"),
            "sexe": record.get("sexe"),
            "last_connection": record.get("last_connection"),
        }

        if location:
            es_doc["location"] = location

        # 👉 Pas de doublons :
        #    _id = id du winker, donc un seul doc par winker dans l'index.
        #    Si tu veux STRICTEMENT ne créer que si absent, remplace "index" par "create".
        actions.append(
            {
                "_op_type": "index",
                "_index": INDEX_NAME,
                "_id": winker_id,
                "_source": es_doc,
            }
        )

    if not actions:
        logging.info("Aucune action à envoyer vers Elasticsearch.")
        return

    logging.info("Indexation de %d winkers dans Elasticsearch...", len(actions))
    helpers.bulk(es, actions)
    logging.info("Indexation terminée.")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@daily",  # à adapter
    catchup=False,
    tags=["winker", "elasticsearch"],
) as dag:
    dag.timezone = PARIS_TZ

    fetch_winkers_from_postgres = PostgresOperator(
        task_id="fetch_winkers_from_postgres",
        postgres_conn_id="postgres_default",  # adapte au conn_id que tu utilises
        sql=SQL_SELECT_WINKERS,
        do_xcom_push=True,
    )

    index_winkers_to_elasticsearch = PythonOperator(
        task_id="index_winkers_to_elasticsearch",
        python_callable=index_winkers_to_es,
    )

    fetch_winkers_from_postgres >> index_winkers_to_elasticsearch
