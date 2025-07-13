from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from typing import Callable

from services.SaturdayActiveEventProcessor import SaturdayActiveEventProcessor
from services.RegularActiveEventProcessor import RegularActiveEventProcessor
# ✅ Fabrique de DAG factorisée
class ScheduledJobFactory:
    def __init__(self, base_dag_id: str, processor_cls: Callable[[], object]):
        self.base_dag_id = base_dag_id
        self.processor_cls = processor_cls

    def create_dag(self, dag_id_suffix: str, cron_schedule: str) -> DAG:
        dag_id = f"{self.base_dag_id}__{dag_id_suffix}"
        processor = self.processor_cls()

        with DAG(
            dag_id=dag_id,
            start_date=datetime(2024, 1, 1),
            schedule_interval=cron_schedule,
            catchup=False,
            tags=["event", "video"]
        ) as dag:
            PythonOperator(
                task_id=f"run_{dag_id_suffix}",
                python_callable=processor.run,
                provide_context=True,
            )

        return dag


# 👷 DAGs réguliers (lundi à vendredi)
regular_factory = ScheduledJobFactory("handle_active_event_videos", RegularActiveEventProcessor)

dag_lundi     = regular_factory.create_dag("lundi_0030",     "30 0 * * 1")
dag_mardi     = regular_factory.create_dag("mardi_2330",     "30 23 * * 2")
dag_mercredi  = regular_factory.create_dag("mercredi_2330",  "30 23 * * 3")
dag_jeudi     = regular_factory.create_dag("jeudi_2330",     "30 23 * * 4")
dag_vendredi  = regular_factory.create_dag("vendredi_2330",  "30 23 * * 5")

# 🧪 DAG spécial samedi
saturday_factory = ScheduledJobFactory("handle_active_event_videos", SaturdayActiveEventProcessor)
dag_samedi = saturday_factory.create_dag("samedi_1400", "0 14 * * 6")
