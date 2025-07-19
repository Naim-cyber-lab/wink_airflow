from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="delete_expired_conversations",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["cleanup", "conversations"],
) as dag:

    # Chargement du contenu SQL depuis le fichier
    with open("/opt/airflow/sql/delete_expired_conversations.sql", "r") as file:
        delete_sql = file.read()

    delete_expired_conversations_task = PostgresOperator(
        task_id="delete_expired_conversations_task",
        postgres_conn_id="my_postgres",  # adapte si ton ID de connexion est différent
        sql=delete_sql,
    )

    delete_expired_conversations_task
