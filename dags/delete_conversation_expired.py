from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

with DAG(
    "delete_expired_conversations",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:

    delete_expired_conversations = PostgresOperator(
        task_id="delete_expired_conversations_task",
        postgres_conn_id="my_postgres",  # à adapter
        sql="sql/delete_expired_conversations.sql"  # ou directement en string via `sql="""..."""`
    )
