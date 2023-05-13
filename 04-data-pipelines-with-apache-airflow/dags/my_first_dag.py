from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="my_first_dag",
    schedule=None,
    start_date=timezone.datetime(2023, 5, 1),
    tags=["DEB", "Skooldio"]
):
    t1 = EmptyOperator(
        task_id="t1"
    )

    t2 = EmptyOperator(
        task_id="t2"
    )

    t1 >> t2
