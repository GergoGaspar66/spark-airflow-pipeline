import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def trigger_bronze():
    # Az importot berakjuk ide a függvény belsejébe!
    sys.path.append('/opt/airflow')
    from scripts.bronze_etl import run_bronze
    return run_bronze()


with DAG(
    dag_id="medallion_full_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    bronze_task = PythonOperator(
        task_id="run_bronze_layer",
        python_callable=trigger_bronze  # Az új belső függvényt hívjuk meg
    )

    bronze_task
