import sys
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src")

from ingestion.ons.carga import ingest_carga
from ingestion.ons.balanco import ingest_balanco

ANO_REFERENCIA = 2026

def executar_carga():
    resultado = ingest_carga(
        year=ANO_REFERENCIA,
        overwrite=False,
    )

    print(f"Curva de carga processada: {resultado}")

def executar_balanco():
    resultado = ingest_balanco(
        year=ANO_REFERENCIA,
        overwrite=False,
    )

    print(f"Balanço de energia processado: {resultado}")

with DAG(
    dag_id="energy_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["energy", "ons"],
) as dag:

    ingest_carga_task = PythonOperator(
        task_id="ingest_carga",
        python_callable=executar_carga,
    )

    ingest_balanco_task = PythonOperator(
        task_id="ingest_balanco",
        python_callable=executar_balanco,
    )

    [ingest_carga_task, ingest_balanco_task]