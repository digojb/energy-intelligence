import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]

CONN_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("ENERGY_DB"),
    "schema": os.getenv("ENERGY_SCHEMA"),
    "role": os.getenv("ENERGY_ROLE"),
}

def conectar_snowflake():
    return snowflake.connector.connect(**CONN_CONFIG)

def carregar_carga(conn, arquivo: Path):

    print(f"\nCarregando: {arquivo.name}")

    df = pd.read_parquet(arquivo)

    ano = int(arquivo.stem.split("_")[-1])

    data_ingestao = datetime.now()

    df["DATA_INGESTAO"] = data_ingestao
    df["FONTE"] = "ONS"
    df["ANO_REFERENCIA"] = ano

    colunas = [
        "id_subsistema",
        "nom_subsistema",
        "din_instante",
        "val_cargaenergiahomwmed",
        "DATA_INGESTAO",
        "FONTE",
        "ANO_REFERENCIA",
    ]

    df = df[colunas]

    dados = list(df.itertuples(index=False, name=None))

    sql = """
        INSERT INTO ENERGY_DB.RAW.CURVA_CARGA (
            ID_SUBSISTEMA,
            NOM_SUBSISTEMA,
            DIN_INSTANTE,
            VAL_CARGAENERGIAHOMWMED,
            DATA_INGESTAO,
            FONTE,
            ANO_REFERENCIA
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    cursor = conn.cursor()

    try:
        cursor.executemany(sql, dados)
        conn.commit()

        print(f"[OK] {len(dados):,} registros carregados.")

    finally:
        cursor.close()

def carregar_balanco(conn, arquivo: Path):

    print(f"\nCarregando: {arquivo.name}")

    df = pd.read_parquet(arquivo)

    ano = int(arquivo.stem.split("_")[-1])

    data_ingestao = datetime.now()

    df["DATA_INGESTAO"] = data_ingestao
    df["FONTE"] = "ONS"
    df["ANO_REFERENCIA"] = ano

    colunas = [
        "id_subsistema",
        "nom_subsistema",
        "din_instante",
        "val_gerhidraulica",
        "val_gertermica",
        "val_gereolica",
        "val_gersolar",
        "val_carga",
        "val_intercambio",
        "DATA_INGESTAO",
        "FONTE",
        "ANO_REFERENCIA",
    ]

    df = df[colunas]

    dados = list(df.itertuples(index=False, name=None))

    sql = """
        INSERT INTO ENERGY_DB.RAW.BALANCO_ENERGIA (
            ID_SUBSISTEMA,
            NOM_SUBSISTEMA,
            DIN_INSTANTE,
            VAL_GERHIDRAULICA,
            VAL_GERTERMICA,
            VAL_GEREOLICA,
            VAL_GERSOLAR,
            VAL_CARGA,
            VAL_INTERCAMBIO,
            DATA_INGESTAO,
            FONTE,
            ANO_REFERENCIA
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
    """

    cursor = conn.cursor()

    try:
        cursor.executemany(sql, dados)
        conn.commit()

        print(f"[OK] {len(dados):,} registros carregados.")

    finally:
        cursor.close()

def testar_conexao():

    print("Conectando ao Snowflake...")

    conn = conectar_snowflake()

    try:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                CURRENT_ACCOUNT(),
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA()
        """)

        resultado = cursor.fetchone()

        print("\nConexão estabelecida!")
        print(f"Account:     {resultado[0]}")
        print(f"User:        {resultado[1]}")
        print(f"Role:        {resultado[2]}")
        print(f"Warehouse:   {resultado[3]}")
        print(f"Database:    {resultado[4]}")
        print(f"Schema:      {resultado[5]}")

        cursor.close()

    finally:
        conn.close()

if __name__ == "__main__":
    testar_conexao()