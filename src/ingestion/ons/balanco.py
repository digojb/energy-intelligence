import argparse
import logging
from io import BytesIO
from pathlib import Path
import pandas as pd
import requests

URL_TEMPLATE = (
    "https://ons-aws-prod-opendata.s3.amazonaws.com/"
    "dataset/balanco_energia_subsistema_ho/"
    "BALANCO_ENERGIA_SUBSISTEMA_{year}.parquet"
)

BASE_DIR = Path(__file__).resolve().parents[3]

RAW_DIR = (
    BASE_DIR
    / "data"
    / "raw"
    / "ons"
    / "balanco"
)

EXPECTED_COLUMNS = [
    "id_subsistema",
    "nom_subsistema",
    "din_instante",
    "val_gerhidraulica",
    "val_gertermica",
    "val_gereolica",
    "val_gersolar",
    "val_carga",
    "val_intercambio",
]

EXPECTED_SUBSYSTEMS = {
    "N",
    "NE",
    "S",
    "SE",
    "SIN",
}

MIN_YEAR = 2020

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

def download_balanco(year: int) -> pd.DataFrame:

    url = URL_TEMPLATE.format(year=year)

    logger.info(
        "Baixando Balanço de Energia - ano %s",
        year
    )

    logger.info("URL: %s", url)

    response = requests.get(
        url,
        timeout=120
    )

    response.raise_for_status()

    logger.info(
        "Download concluído: %.2f MB",
        len(response.content) / 1024 / 1024
    )

    df = pd.read_parquet(
        BytesIO(response.content)
    )

    logger.info(
        "Registros carregados: %s",
        len(df)
    )

    return df

def normalize_balanco(df: pd.DataFrame) -> pd.DataFrame:

    logger.info("Normalizando dados...")

    df = df.copy()

    # Identificadores
    df["id_subsistema"] = (
        df["id_subsistema"]
        .astype("string")
        .str.strip()
    )

    df["nom_subsistema"] = (
        df["nom_subsistema"]
        .astype("string")
        .str.strip()
    )

    # Data/hora
    df["din_instante"] = pd.to_datetime(
        df["din_instante"],
        errors="coerce"
    )

    # Métricas numéricas
    colunas_numericas = [
        "val_gerhidraulica",
        "val_gertermica",
        "val_gereolica",
        "val_gersolar",
        "val_carga",
        "val_intercambio",
    ]

    for coluna in colunas_numericas:

        tipo_original = df[coluna].dtype

        df[coluna] = pd.to_numeric(
            df[coluna],
            errors="coerce"
        )

        logger.info(
            "%s: %s -> %s",
            coluna,
            tipo_original,
            df[coluna].dtype
        )

    return df

def validate_balanco(df: pd.DataFrame):

    logger.info("Iniciando validação...")

    # --------------------------------------------------------
    # Dataset vazio
    # --------------------------------------------------------

    if df.empty:

        raise ValueError(
            "Dataset vazio."
        )

    # --------------------------------------------------------
    # Colunas
    # --------------------------------------------------------

    if list(df.columns) != EXPECTED_COLUMNS:

        raise ValueError(
            "Schema inesperado.\n"
            f"Esperado: {EXPECTED_COLUMNS}\n"
            f"Encontrado: {df.columns.tolist()}"
        )

    # --------------------------------------------------------
    # Nulos
    # --------------------------------------------------------

    nulos = df.isna().sum()

    if nulos.sum() > 0:

        raise ValueError(
            "Foram encontrados valores nulos:\n"
            f"{nulos[nulos > 0]}"
        )

    # --------------------------------------------------------
    # Tipos
    # --------------------------------------------------------

    if not pd.api.types.is_datetime64_any_dtype(
        df["din_instante"]
    ):

        raise TypeError(
            "din_instante não é datetime."
        )

    colunas_numericas = [
        "val_gerhidraulica",
        "val_gertermica",
        "val_gereolica",
        "val_gersolar",
        "val_carga",
        "val_intercambio",
    ]

    for coluna in colunas_numericas:

        if not pd.api.types.is_numeric_dtype(
            df[coluna]
        ):

            raise TypeError(
                f"{coluna} não é numérico."
            )

    # --------------------------------------------------------
    # Subsistemas
    # --------------------------------------------------------

    subsistemas = set(
        df["id_subsistema"].unique()
    )

    inesperados = (
        subsistemas - EXPECTED_SUBSYSTEMS
    )

    if inesperados:

        raise ValueError(
            "Subsistemas inesperados: "
            f"{sorted(inesperados)}"
        )

    # --------------------------------------------------------
    # Duplicidade completa
    # --------------------------------------------------------

    duplicados = df.duplicated().sum()

    if duplicados > 0:

        raise ValueError(
            f"Foram encontradas "
            f"{duplicados} linhas duplicadas."
        )

    # --------------------------------------------------------
    # Chave natural
    # --------------------------------------------------------

    chave = [
        "id_subsistema",
        "din_instante"
    ]

    duplicados_chave = df.duplicated(
        subset=chave
    ).sum()

    if duplicados_chave > 0:

        raise ValueError(
            "Duplicidade encontrada na "
            "chave natural "
            "(id_subsistema + din_instante): "
            f"{duplicados_chave}"
        )

    # --------------------------------------------------------
    # Valores negativos
    # --------------------------------------------------------

    TOLERANCIA_GERACAO = -1.0

    # Geração e carga não devem ser negativas.
    colunas_nao_negativas = [
        "val_gerhidraulica",
        "val_gertermica",
        "val_gereolica",
        "val_gersolar",
        "val_carga",
    ]

    for coluna in colunas_nao_negativas:

        valores_invalidos = (
            df[coluna] < TOLERANCIA_GERACAO
        ).sum()

        quantidade_invalidos = valores_invalidos.sum()

        if quantidade_invalidos > 0:
            raise ValueError(
                f"{coluna} possui "
                f"{quantidade_invalidos} valores abaixo da "
                f"tolerância de {TOLERANCIA_GERACAO} MWmed."
            )

    # --------------------------------------------------------
    # Frequência temporal
    # --------------------------------------------------------

    gaps_total = 0

    for subsistema, grupo in df.groupby(
        "id_subsistema"
    ):

        datas = (
            grupo["din_instante"]
            .sort_values()
        )

        diferencas = (
            datas.diff()
            .dropna()
        )

        gaps = diferencas[
            diferencas != pd.Timedelta(hours=1)
        ]

        if len(gaps) > 0:

            gaps_total += len(gaps)

            logger.warning(
                "Subsistema %s possui %s "
                "intervalos diferentes de 1 hora.",
                subsistema,
                len(gaps)
            )

    if gaps_total == 0:

        logger.info(
            "Frequência temporal consistente: "
            "1 hora."
        )

    # --------------------------------------------------------
    # Informações finais
    # --------------------------------------------------------

    logger.info(
        "Período: %s -> %s",
        df["din_instante"].min(),
        df["din_instante"].max()
    )

    logger.info(
        "Registros: %s",
        len(df)
    )

    logger.info(
        "Subsistemas: %s",
        sorted(subsistemas)
    )

    logger.info(
        "Validação concluída com sucesso."
    )

def save_raw(df: pd.DataFrame, year: int):

    RAW_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_path = (
        RAW_DIR
        / f"balanco_{year}.parquet"
    )

    df.to_parquet(
        output_path,
        index=False,
        engine="pyarrow"
    )

    logger.info(
        "Arquivo RAW salvo: %s",
        output_path
    )

def ingest_balanco(year: int, overwrite: bool = False):

    if year < MIN_YEAR:

        raise ValueError(
            f"Ano inválido: {year}. "
            f"Mínimo permitido: {MIN_YEAR}."
        )

    output_path = (
        RAW_DIR
        / f"balanco_{year}.parquet"
    )

    # --------------------------------------------------------
    # Idempotência
    # --------------------------------------------------------

    if output_path.exists() and not overwrite:

        logger.info(
            "Arquivo RAW já existe: %s",
            output_path
        )

        logger.info(
            "Nenhum download será realizado."
        )

        return

    # --------------------------------------------------------
    # Pipeline
    # --------------------------------------------------------

    df = download_balanco(year)

    df = normalize_balanco(df)

    validate_balanco(df)

    save_raw(
        df,
        year
    )

def ingest_period( start_year: int, end_year: int, overwrite: bool = False
):

    if start_year > end_year:

        raise ValueError(
            "start_year não pode ser maior "
            "que end_year."
        )

    sucessos = []
    falhas = []

    for year in range(
        start_year,
        end_year + 1
    ):

        print(
            f"\n{'=' * 70}"
        )

        print(
            f"Processando ano: {year}"
        )

        print(
            f"{'=' * 70}"
        )

        try:

            ingest_balanco(
                year=year,
                overwrite=overwrite
            )

            sucessos.append(year)

        except Exception as exc:

            logger.exception(
                "Falha ao processar %s",
                year
            )

            falhas.append(
                (year, str(exc))
            )

    # --------------------------------------------------------
    # Resumo
    # --------------------------------------------------------

    print("\n")
    print("=" * 70)
    print("INGESTÃO HISTÓRICA FINALIZADA")
    print("=" * 70)

    print(
        f"Anos processados com sucesso: "
        f"{sucessos}"
    )

    print(
        f"Anos com falha: "
        f"{[ano for ano, _ in falhas]}"
    )

    if falhas:

        print("\nDetalhes das falhas:")

        for ano, erro in falhas:

            print(
                f"- {ano}: {erro}"
            )

        raise RuntimeError(
            "Um ou mais anos falharam."
        )

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Ingestão dos dados de "
            "Balanço de Energia do ONS."
        )
    )

    parser.add_argument(
        "--year",
        type=int,
        help="Processa apenas um ano."
    )

    parser.add_argument(
        "--start-year",
        type=int,
        help="Ano inicial."
    )

    parser.add_argument(
        "--end-year",
        type=int,
        help="Ano final."
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescreve arquivos existentes."
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    # Um único ano
    # --------------------------------------------------------

    if args.year:

        ingest_balanco(
            year=args.year,
            overwrite=args.overwrite
        )

    # --------------------------------------------------------
    # Período
    # --------------------------------------------------------

    elif (
        args.start_year
        and args.end_year
    ):

        ingest_period(
            start_year=args.start_year,
            end_year=args.end_year,
            overwrite=args.overwrite
        )

    # --------------------------------------------------------
    # Sem argumentos
    # --------------------------------------------------------

    else:

        current_year = pd.Timestamp.now().year

        ingest_period(
            start_year=MIN_YEAR,
            end_year=current_year,
            overwrite=args.overwrite
        )