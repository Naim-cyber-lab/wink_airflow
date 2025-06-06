from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def get_conversations(**kwargs):
    return "Bonjour depuis t1"  # Cette valeur sera automatiquement "poussée" dans les XCom

def generate_summary(**kwargs):
    ti = kwargs["ti"]  # ti = task instance
    message = ti.xcom_pull(task_ids="t1")  # On récupère la valeur de t1
    print(f"Valeur reçue de t1 : {message}")

with DAG(
    dag_id="llm_conversations",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 1 * * *",  # ✅ tous les jours à 1h du matin
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="t1",
        python_callable=get_conversations,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id="t2",
        python_callable=generate_summary,
        provide_context=True
    )

    t1 >> t2
