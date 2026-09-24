"""
Ingestão da Curva de Carga Horária do ONS.

Responsabilidades:
    1. Baixar os dados da Curva de Carga do ONS.
    2. Normalizar diferenças de tipos entre anos.
    3. Validar a estrutura e qualidade dos dados.
    4. Salvar os dados brutos (RAW) localmente.
    5. Permitir ingestão de um ou vários anos.
    6. Evitar download de arquivos que já existem.
    7. Permitir reprocessamento utilizando --overwrite.

Exemplos:

    # Um ano
    python src/ingestion/ons/carga.py --year 2025

    # Histórico completo
    python src/ingestion/ons/carga.py --start-year 2020 --end-year 2026

    # Reprocessar histórico
    python src/ingestion/ons/carga.py \
        --start-year 2020 \
        --end-year 2026 \
        --overwrite
"""

from __future__ import annotations
import argparse
import logging
from io import BytesIO
from pathlib import Path
import pandas as pd
import requests

URL_TEMPLATE = (
    "https://ons-aws-prod-opendata.s3.amazonaws.com/"
    "dataset/curva-carga-ho/CURVA_CARGA_{year}.parquet"
)

BASE_DIR = Path(__file__).resolve().parents[3]

RAW_DIR = (
    BASE_DIR
    / "data"
    / "raw"
    / "ons"
    / "carga"
)

EXPECTED_COLUMNS = [
    "id_subsistema",
    "nom_subsistema",
    "din_instante",
    "val_cargaenergiahomwmed",
]

EXPECTED_SUBSYSTEMS = {
    "N",
    "NE",
    "S",
    "SE",
}

MIN_YEAR = 2020

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

def download_carga(year: int) -> pd.DataFrame:

    if not isinstance(year, int):
        raise ValueError(
            "O parâmetro 'year' deve ser um inteiro."
        )

    if year < MIN_YEAR:
        raise ValueError(
            f"O ano deve ser maior ou igual a {MIN_YEAR}. "
            f"Recebido: {year}"
        )

    url = URL_TEMPLATE.format(year=year)

    logger.info(
        "Iniciando download da Curva de Carga - ano %s",
        year,
    )

    logger.info(
        "URL: %s",
        url,
    )

    try:
        response = requests.get(
            url,
            timeout=120,
        )

        response.raise_for_status()

    except requests.RequestException as exc:
        logger.error(
            "Erro ao acessar os dados do ONS para %s: %s",
            year,
            exc,
        )
        raise

    logger.info(
        "Download concluído. Tamanho: %.2f MB",
        len(response.content) / 1024 / 1024,
    )

    try:
        df = pd.read_parquet(
            BytesIO(response.content)
        )

    except Exception as exc:
        logger.error(
            "Erro ao interpretar o Parquet de %s: %s",
            year,
            exc,
        )
        raise

    logger.info(
        "Dados carregados: %s linhas x %s colunas",
        df.shape[0],
        df.shape[1],
    )

    return df

def normalize_carga(df: pd.DataFrame,) -> pd.DataFrame:
    """
    Normaliza os tipos das colunas da Curva de Carga.

    O ONS utiliza representações diferentes dependendo do ano.

    Exemplos:

        2020:
            val_cargaenergiahomwmed -> str

        2026:
            val_cargaenergiahomwmed -> float64

    Os valores são normalizados para um schema consistente.

    Returns
    -------
    pd.DataFrame
        DataFrame normalizado.
    """

    logger.info(
        "Iniciando normalização dos dados."
    )

    df = df.copy()

    # --------------------------------------------------------
    # Subsistema
    # --------------------------------------------------------

    df["id_subsistema"] = (
        df["id_subsistema"]
        .astype("string")
        .str.strip()
    )

    # --------------------------------------------------------
    # Nome do subsistema
    # --------------------------------------------------------

    df["nom_subsistema"] = (
        df["nom_subsistema"]
        .astype("string")
        .str.strip()
    )

    # --------------------------------------------------------
    # Data/hora
    # --------------------------------------------------------

    df["din_instante"] = pd.to_datetime(
        df["din_instante"],
        errors="coerce",
    )

    # --------------------------------------------------------
    # Carga
    # --------------------------------------------------------

    original_dtype = (
        df["val_cargaenergiahomwmed"].dtype
    )

    df["val_cargaenergiahomwmed"] = (
        pd.to_numeric(
            df["val_cargaenergiahomwmed"],
            errors="coerce",
        )
    )

    logger.info(
        "Tipo original da carga: %s",
        original_dtype,
    )

    logger.info(
        "Tipo normalizado da carga: %s",
        df["val_cargaenergiahomwmed"].dtype,
    )

    logger.info(
        "Normalização concluída."
    )

    return df

def validate_carga(df: pd.DataFrame,) -> None:
    """
    Executa as validações de qualidade da Curva de Carga.

    Raises
    ------
    ValueError
        Caso alguma regra crítica seja violada.
    """

    logger.info(
        "Iniciando validações da Curva de Carga."
    )

    # --------------------------------------------------------
    # 1. DataFrame vazio
    # --------------------------------------------------------

    if df.empty:
        raise ValueError(
            "O DataFrame da Curva de Carga está vazio."
        )

    # --------------------------------------------------------
    # 2. Colunas obrigatórias
    # --------------------------------------------------------

    missing_columns = (
        set(EXPECTED_COLUMNS)
        - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Colunas obrigatórias ausentes: "
            f"{missing_columns}"
        )

    # --------------------------------------------------------
    # 3. Valores nulos
    # --------------------------------------------------------

    null_counts = (
        df[EXPECTED_COLUMNS]
        .isnull()
        .sum()
    )

    columns_with_nulls = (
        null_counts[null_counts > 0]
    )

    if not columns_with_nulls.empty:
        raise ValueError(
            "Foram encontrados valores nulos:\n"
            f"{columns_with_nulls}"
        )

    # --------------------------------------------------------
    # 4. Tipo da data
    # --------------------------------------------------------

    if not pd.api.types.is_datetime64_any_dtype(
        df["din_instante"]
    ):
        raise ValueError(
            "A coluna 'din_instante' "
            "não está em formato datetime."
        )

    # --------------------------------------------------------
    # 5. Tipo da carga
    # --------------------------------------------------------

    if not pd.api.types.is_numeric_dtype(
        df["val_cargaenergiahomwmed"]
    ):
        raise ValueError(
            "A coluna 'val_cargaenergiahomwmed' "
            "não possui tipo numérico."
        )

    # --------------------------------------------------------
    # 6. Subsistemas
    # --------------------------------------------------------

    actual_subsystems = set(
        df["id_subsistema"].unique()
    )

    unexpected_subsystems = (
        actual_subsystems
        - EXPECTED_SUBSYSTEMS
    )

    if unexpected_subsystems:
        raise ValueError(
            "Foram encontrados subsistemas "
            f"não esperados: {unexpected_subsystems}"
        )

    # --------------------------------------------------------
    # 7. Linhas completamente duplicadas
    # --------------------------------------------------------

    duplicated_rows = df.duplicated().sum()

    if duplicated_rows > 0:
        raise ValueError(
            f"Foram encontradas {duplicated_rows} "
            "linhas completamente duplicadas."
        )

    # --------------------------------------------------------
    # 8. Chave natural
    # --------------------------------------------------------

    natural_key = [
        "id_subsistema",
        "din_instante",
    ]

    duplicated_keys = (
        df.duplicated(
            subset=natural_key
        )
        .sum()
    )

    if duplicated_keys > 0:
        raise ValueError(
            f"Foram encontradas {duplicated_keys} "
            "duplicidades na chave natural: "
            f"{natural_key}"
        )

    # --------------------------------------------------------
    # 9. Carga negativa
    # --------------------------------------------------------

    negative_load = (
        df["val_cargaenergiahomwmed"] < 0
    ).sum()

    if negative_load > 0:
        raise ValueError(
            f"Foram encontrados {negative_load} "
            "valores de carga negativos."
        )

    # --------------------------------------------------------
    # 10. Frequência temporal
    # --------------------------------------------------------

    ordered = df.sort_values(
        [
            "id_subsistema",
            "din_instante",
        ]
    )

    intervals = (
        ordered
        .groupby("id_subsistema")["din_instante"]
        .diff()
        .dropna()
    )

    invalid_intervals = intervals[
        intervals != pd.Timedelta(hours=1)
    ]

    if not invalid_intervals.empty:
        logger.warning(
            "Foram encontrados %s intervalos "
            "diferentes de 1 hora.",
            len(invalid_intervals),
        )

    # --------------------------------------------------------
    # 11. Estatísticas
    # --------------------------------------------------------

    logger.info(
        "Período: %s até %s",
        df["din_instante"].min(),
        df["din_instante"].max(),
    )

    logger.info(
        "Registros: %s",
        len(df),
    )

    logger.info(
        "Subsistemas: %s",
        sorted(actual_subsystems),
    )

    logger.info(
        "Validação concluída com sucesso."
    )

def save_raw(df: pd.DataFrame,year: int,) -> Path:
    """
    Salva os dados da Curva de Carga em Parquet.
    """

    RAW_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        RAW_DIR
        / f"carga_{year}.parquet"
    )

    logger.info(
        "Salvando dados RAW em: %s",
        output_path,
    )

    df.to_parquet(
        output_path,
        index=False,
        engine="pyarrow",
    )

    logger.info(
        "Arquivo RAW salvo com sucesso."
    )

    return output_path

def ingest_carga(year: int,overwrite: bool = False,) -> Path:
    """
    Executa a ingestão completa de um ano.
    """

    logger.info(
        "=" * 60
    )

    logger.info(
        "INICIANDO INGESTÃO DA CURVA DE CARGA - %s",
        year,
    )

    logger.info(
        "=" * 60
    )

    output_path = (
        RAW_DIR
        / f"carga_{year}.parquet"
    )

    # --------------------------------------------------------
    # Idempotência
    # --------------------------------------------------------

    if output_path.exists() and not overwrite:

        logger.info(
            "Arquivo RAW já existe: %s",
            output_path,
        )

        logger.info(
            "Nenhum download será realizado."
        )

        return output_path

    # --------------------------------------------------------
    # Download
    # --------------------------------------------------------

    df = download_carga(year)

    # --------------------------------------------------------
    # Normalização
    # --------------------------------------------------------

    df = normalize_carga(df)

    # --------------------------------------------------------
    # Validação
    # --------------------------------------------------------

    validate_carga(df)

    # --------------------------------------------------------
    # Salvamento
    # --------------------------------------------------------

    output_path = save_raw(
        df,
        year,
    )

    logger.info(
        "=" * 60
    )

    logger.info(
        "INGESTÃO CONCLUÍDA COM SUCESSO - %s",
        year,
    )

    logger.info(
        "Arquivo: %s",
        output_path,
    )

    logger.info(
        "=" * 60
    )

    return output_path

def ingest_period(start_year: int,end_year: int,overwrite: bool = False,) -> None:
    """
    Executa a ingestão de todos os anos de um intervalo.
    """

    if start_year > end_year:
        raise ValueError(
            "start_year não pode ser maior que end_year."
        )

    if start_year < MIN_YEAR:
        raise ValueError(
            f"O ano inicial deve ser >= {MIN_YEAR}."
        )

    total_years = (
        end_year - start_year + 1
    )

    logger.info(
        "=" * 70
    )

    logger.info(
        "INICIANDO INGESTÃO HISTÓRICA"
    )

    logger.info(
        "Período: %s - %s",
        start_year,
        end_year,
    )

    logger.info(
        "Total de anos: %s",
        total_years,
    )

    logger.info(
        "=" * 70
    )

    successful_years = []
    failed_years = []

    for index, year in enumerate(range(start_year, end_year + 1),start=1,):

        logger.info(
            "PROCESSANDO ANO %s/%s: %s",
            index,
            total_years,
            year,
        )

        try:

            ingest_carga(
                year=year,
                overwrite=overwrite,
            )

            successful_years.append(year)

        except Exception as exc:

            logger.error(
                "Falha ao processar o ano %s: %s",
                year,
                exc,
            )

            failed_years.append(year)

    # --------------------------------------------------------
    # Resumo
    # --------------------------------------------------------

    logger.info(
        "=" * 70
    )

    logger.info(
        "INGESTÃO HISTÓRICA FINALIZADA"
    )

    logger.info(
        "Anos processados com sucesso: %s",
        successful_years,
    )

    logger.info(
        "Anos com falha: %s",
        failed_years,
    )

    logger.info(
        "=" * 70
    )

    if failed_years:

        raise RuntimeError(
            "A ingestão histórica terminou com "
            f"falhas nos anos: {failed_years}"
        )

def parse_arguments() -> argparse.Namespace:
    """
    Configura os argumentos da linha de comando.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Ingestão da Curva de Carga Horária do ONS."
        )
    )

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "--year",
        type=int,
        help="Processa somente um ano.",
    )

    group.add_argument(
        "--start-year",
        type=int,
        help="Primeiro ano do intervalo.",
    )

    parser.add_argument(
        "--end-year",
        type=int,
        help=(
            "Último ano do intervalo. "
            "Obrigatório quando --start-year "
            "for utilizado."
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Rebaixa e substitui arquivos "
            "RAW existentes."
        ),
    )

    return parser.parse_args()

if __name__ == "__main__":

    args = parse_arguments()

    # --------------------------------------------------------
    # Um único ano
    # --------------------------------------------------------

    if args.year is not None:

        ingest_carga(
            year=args.year,
            overwrite=args.overwrite,
        )

    # --------------------------------------------------------
    # Intervalo
    # --------------------------------------------------------

    elif args.start_year is not None:

        if args.end_year is None:
            raise ValueError(
                "--end-year é obrigatório quando "
                "--start-year é utilizado."
            )

        ingest_period(
            start_year=args.start_year,
            end_year=args.end_year,
            overwrite=args.overwrite,
        )

    # --------------------------------------------------------
    # Nenhum argumento
    # --------------------------------------------------------

    else:

        current_year = pd.Timestamp.now().year

        ingest_period(
            start_year=MIN_YEAR,
            end_year=current_year,
            overwrite=args.overwrite,
        )

