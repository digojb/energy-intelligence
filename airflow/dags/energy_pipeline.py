
import sys
from pathlib import Path
from datetime import datetime
import subprocess
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

ANO_REFERENCIA = datetime.now().year

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

    print(
        f"[INICIO] Carga da curva de carga no Snowflake. "
        f"Ano: {ANO_REFERENCIA} | Arquivo: {arquivo}"
    )

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo de curva de carga não encontrado: {arquivo}"
        )

    conn = None

    try:
        conn = conectar_snowflake()
        print("[INFO] Conexão com o Snowflake estabelecida.")

        carregar_carga(conn, arquivo)

        print(
            f"[SUCESSO] Curva de carga carregada no Snowflake. "
            f"Ano: {ANO_REFERENCIA} | Arquivo: {arquivo.name}"
        )

    except Exception as erro:
        print(
            f"[ERRO] Falha ao carregar a curva de carga no Snowflake. "
            f"Ano: {ANO_REFERENCIA} | Erro: {erro}"
        )
        raise

    finally:
        if conn is not None:
            conn.close()
            print("[INFO] Conexão com o Snowflake encerrada.")


def carregar_balanco_snowflake():
    arquivo = PASTA_BALANCO / f"balanco_{ANO_REFERENCIA}.parquet"

    print(
        f"[INICIO] Carga do balanço de energia no Snowflake. "
        f"Ano: {ANO_REFERENCIA} | Arquivo: {arquivo}"
    )

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo de balanço não encontrado: {arquivo}"
        )

    conn = None

    try:
        conn = conectar_snowflake()
        print("[INFO] Conexão com o Snowflake estabelecida.")

        carregar_balanco(conn, arquivo)

        print(
            f"[SUCESSO] Balanço de energia carregado no Snowflake. "
            f"Ano: {ANO_REFERENCIA} | Arquivo: {arquivo.name}"
        )

    except Exception as erro:
        print(
            f"[ERRO] Falha ao carregar o balanço de energia no Snowflake. "
            f"Ano: {ANO_REFERENCIA} | Erro: {erro}"
        )
        raise

    finally:
        if conn is not None:
            conn.close()
            print("[INFO] Conexão com o Snowflake encerrada.")


def executar_dbt_build():
    comando = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "build",
        "--project-dir",
        "/opt/airflow/dbt/energy",
        "--profiles-dir",
        "/opt/airflow/dbt_profiles",
    ]

    resultado = subprocess.run(
        comando,
        capture_output=True,
        text=True,
    )

    print(resultado.stdout)

    if resultado.stderr:
        print(resultado.stderr)

    if resultado.returncode != 0:
        raise RuntimeError(
            f"dbt build falhou com código {resultado.returncode}"
        )
    
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

    dbt_build_task = PythonOperator(
        task_id="dbt_build",
        python_callable=executar_dbt_build,
    )

    ingest_carga_task >> load_carga_task
    ingest_balanco_task >> load_balanco_task

    [load_carga_task, load_balanco_task] >> dbt_build_task
