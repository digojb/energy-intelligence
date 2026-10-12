
import sys
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src")

from ingestion.ons.carga import ingest_carga
from ingestion.ons.balanco import ingest_balanco
from ingestion.loaders.snowflake_loader import (
    conectar_snowflake,
    carregar_carga,
    carregar_balanco,
)

ANO_REFERENCIA = 2026

PASTA_CARGA = Path("/opt/airflow/data/raw/ons/carga")
PASTA_BALANCO = Path("/opt/airflow/data/raw/ons/balanco")


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


def carregar_carga_snowflake():
    arquivo = PASTA_CARGA / f"carga_{ANO_REFERENCIA}.parquet"

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo de curva de carga não encontrado: {arquivo}"
        )

    conn = conectar_snowflake()

    try:
        carregar_carga(conn, arquivo)
    finally:
        conn.close()


def carregar_balanco_snowflake():
    arquivo = PASTA_BALANCO / f"balanco_{ANO_REFERENCIA}.parquet"

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo de balanço não encontrado: {arquivo}"
        )

    conn = conectar_snowflake()

    try:
        carregar_balanco(conn, arquivo)
    finally:
        conn.close()


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

    load_carga_task = PythonOperator(
        task_id="load_carga_snowflake",
        python_callable=carregar_carga_snowflake,
    )

    load_balanco_task = PythonOperator(
        task_id="load_balanco_snowflake",
        python_callable=carregar_balanco_snowflake,
    )

    ingest_carga_task >> load_carga_task
    ingest_balanco_task >> load_balanco_task
