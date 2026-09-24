import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]

RAW_DIR = (
    BASE_DIR
    / "data"
    / "raw"
    / "ons"
    / "balanco"
)

ANOS = range(2020, 2027)

COLUNAS_ESPERADAS = [
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

SUBSISTEMAS_ESPERADOS = {
    "N",
    "NE",
    "S",
    "SE",
    "SIN",
}

COLUNAS_NAO_NEGATIVAS = [
    "val_gerhidraulica",
    "val_gertermica",
    "val_gereolica",
    "val_gersolar",
    "val_carga",
]

print("=" * 75)
print("       PERFIL HISTÓRICO - BALANÇO DE ENERGIA ONS")
print("=" * 75)


resultados = []

schema_referencia = None

total_registros = 0
total_nulos = 0
total_duplicados = 0
total_chave_duplicada = 0
total_negativos = 0
total_gaps = 0

for ano in ANOS:

    print("\n" + "=" * 75)
    print(f"ANO: {ano}")
    print("=" * 75)

    arquivo = (
        RAW_DIR
        / f"balanco_{ano}.parquet"
    )

    # --------------------------------------------------------
    # ARQUIVO
    # --------------------------------------------------------

    if not arquivo.exists():

        print(
            f"[ERRO] Arquivo não encontrado: "
            f"{arquivo}"
        )

        continue

    print(
        f"Arquivo: {arquivo.name}"
    )

    # --------------------------------------------------------
    # LEITURA
    # --------------------------------------------------------

    df = pd.read_parquet(
        arquivo
    )

    registros = len(df)

    total_registros += registros

    print(
        f"Registros: {registros:,}"
    )

    # --------------------------------------------------------
    # SCHEMA
    # --------------------------------------------------------

    colunas = df.columns.tolist()

    if schema_referencia is None:

        schema_referencia = colunas

    schema_ok = (
        colunas == schema_referencia
        and colunas == COLUNAS_ESPERADAS
    )

    print(
        "Schema consistente:",
        "SIM" if schema_ok else "NAO"
    )

    if not schema_ok:

        print(
            "Colunas encontradas:"
        )

        print(colunas)

    # --------------------------------------------------------
    # TIPOS
    # --------------------------------------------------------

    print("\nTipos:")

    for coluna in df.columns:

        print(
            f"  {coluna}: "
            f"{df[coluna].dtype}"
        )

    # --------------------------------------------------------
    # NULOS
    # --------------------------------------------------------

    nulos = int(
        df.isna()
        .sum()
        .sum()
    )

    total_nulos += nulos

    print(
        f"\nNulos: {nulos}"
    )

    if nulos > 0:

        print(
            df.isna()
            .sum()
            .loc[
                lambda x: x > 0
            ]
        )

    # --------------------------------------------------------
    # DUPLICIDADES COMPLETAS
    # --------------------------------------------------------

    duplicados = int(
        df.duplicated()
        .sum()
    )

    total_duplicados += duplicados

    print(
        f"Linhas duplicadas: "
        f"{duplicados}"
    )

    # --------------------------------------------------------
    # CHAVE NATURAL
    # --------------------------------------------------------

    chave = [
        "id_subsistema",
        "din_instante",
    ]

    duplicados_chave = int(
        df.duplicated(
            subset=chave
        ).sum()
    )

    total_chave_duplicada += (
        duplicados_chave
    )

    print(
        "Duplicidades da chave "
        "(id_subsistema + din_instante):",
        duplicados_chave
    )

    # --------------------------------------------------------
    # PERÍODO
    # --------------------------------------------------------

    inicio = (
        df["din_instante"]
        .min()
    )

    fim = (
        df["din_instante"]
        .max()
    )

    print(
        f"Período: "
        f"{inicio} -> {fim}"
    )

    # --------------------------------------------------------
    # SUBSISTEMAS
    # --------------------------------------------------------

    subsistemas = set(
        df["id_subsistema"]
        .dropna()
        .unique()
    )

    inesperados = (
        subsistemas
        - SUBSISTEMAS_ESPERADOS
    )

    ausentes = (
        SUBSISTEMAS_ESPERADOS
        - subsistemas
    )

    print(
        "Subsistemas:",
        sorted(subsistemas)
    )

    if inesperados:

        print(
            "[ALERTA] Subsistemas "
            f"inesperados: "
            f"{sorted(inesperados)}"
        )

    if ausentes:

        print(
            "[ALERTA] Subsistemas "
            f"ausentes: "
            f"{sorted(ausentes)}"
        )

    # --------------------------------------------------------
    # VALORES NEGATIVOS
    # --------------------------------------------------------

    negativos_ano = 0

    print("\nValores negativos:")

    for coluna in COLUNAS_NAO_NEGATIVAS:

        negativos = int(
            (df[coluna] < 0)
            .sum()
        )

        negativos_ano += negativos

        if negativos > 0:

            print(
                f"  {coluna}: "
                f"{negativos}"
            )

    # Intercâmbio é permitido negativo
    negativos_intercambio = int(
        (df["val_intercambio"] < 0)
        .sum()
    )

    print(
        f"  val_intercambio: "
        f"{negativos_intercambio} "
        f"(permitidos)"
    )

    total_negativos += (
        negativos_ano
    )

    # --------------------------------------------------------
    # REGISTROS POR SUBSISTEMA
    # --------------------------------------------------------

    print(
        "\nRegistros por subsistema:"
    )

    contagem_subsistemas = (
        df.groupby(
            [
                "id_subsistema",
                "nom_subsistema",
            ]
        )
        .size()
        .sort_index()
    )

    print(
        contagem_subsistemas
        .to_string()
    )

    # --------------------------------------------------------
    # FREQUÊNCIA TEMPORAL
    # --------------------------------------------------------

    gaps_ano = 0

    print(
        "\nVerificação temporal:"
    )

    for subsistema, grupo in (
        df.groupby("id_subsistema")
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
            diferencas
            != pd.Timedelta(hours=1)
        ]

        quantidade_gaps = len(gaps)

        gaps_ano += quantidade_gaps

        if quantidade_gaps > 0:

            print(
                f"  {subsistema}: "
                f"{quantidade_gaps} "
                f"intervalo(s) "
                f"diferente(s) de 1 hora"
            )

    if gaps_ano == 0:

        print(
            "  OK - frequência "
            "horária consistente"
        )

    total_gaps += gaps_ano

    # --------------------------------------------------------
    # RESULTADO DO ANO
    # --------------------------------------------------------

    resultados.append(
        {
            "ano": ano,
            "registros": registros,
            "inicio": inicio,
            "fim": fim,
            "nulos": nulos,
            "duplicados": duplicados,
            "chave_duplicada": (
                duplicados_chave
            ),
            "negativos": (
                negativos_ano
            ),
            "gaps": gaps_ano,
            "subsistemas": len(
                subsistemas
            ),
            "schema_ok": schema_ok,
        }
    )

print("\n\n")
print("=" * 75)
print("                  RESUMO HISTÓRICO")
print("=" * 75)


if resultados:

    resumo = pd.DataFrame(
        resultados
    )

    print("\n")

    print(
        resumo[
            [
                "ano",
                "registros",
                "inicio",
                "fim",
                "nulos",
                "duplicados",
                "chave_duplicada",
                "negativos",
                "gaps",
            ]
        ].to_string(
            index=False
        )
    )

    print("\n" + "-" * 75)

    print(
        f"TOTAL DE REGISTROS: "
        f"{total_registros:,}"
    )

    print(
        f"TOTAL DE NULOS: "
        f"{total_nulos:,}"
    )

    print(
        f"TOTAL DE DUPLICIDADES: "
        f"{total_duplicados:,}"
    )

    print(
        "TOTAL DE DUPLICIDADES "
        f"DA CHAVE: "
        f"{total_chave_duplicada:,}"
    )

    print(
        "TOTAL DE VALORES NEGATIVOS "
        f"EM MÉTRICAS NÃO NEGATIVAS: "
        f"{total_negativos:,}"
    )

    print(
        f"TOTAL DE GAPS: "
        f"{total_gaps:,}"
    )

    print(
        "SCHEMA CONSISTENTE: "
        f"{'SIM' if resumo['schema_ok'].all() else 'NAO'}"
    )

    print(
        "SUBSISTEMAS ESPERADOS: "
        f"{sorted(SUBSISTEMAS_ESPERADOS)}"
    )

    print("\n" + "=" * 75)

    problemas = (
        total_nulos > 0
        or total_duplicados > 0
        or total_chave_duplicada > 0
        or total_negativos > 0
        or total_gaps > 0
        or not resumo["schema_ok"].all()
    )

    if problemas:

        print(
            "STATUS: ATENÇÃO - "
            "existem inconsistências."
        )

    else:

        print(
            "STATUS: OK - dados históricos "
            "validados com sucesso."
        )

    print("=" * 75)

else:

    print(
        "[ERRO] Nenhum arquivo encontrado."
    )