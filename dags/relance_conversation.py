# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
#
# from env import DATABASE_URL  # ← à adapter
#
# def relancer_conversations():
#     engine = create_engine(DATABASE_URL)
#     Session = sessionmaker(bind=engine)
#     db = Session()
#
#     relance = RelanceConversation(model_name="mistral")
#     relance.relancer(db)
#
#     db.close()
#
# default_args = {
#     'owner': 'wink',
#     'depends_on_past': False,
#     'email_on_failure': True,
#     'email': ['admin@tonapp.com'],
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# with DAG(
#     dag_id='relance_conversation_dag',
#     default_args=default_args,
#     description='Relance automatique des conversations inactives via IA',
#     schedule_interval='0 4 * * *',  # tous les jours à 4h du matin
#     start_date=datetime(2024, 6, 28),
#     catchup=False,
#     tags=['wink', 'conversation', 'ia'],
# ) as dag:
#
#     relancer = PythonOperator(
#         task_id='relancer_les_conversations_inactives',
#         python_callable=relancer_conversations
#     )
#
#     relancer
