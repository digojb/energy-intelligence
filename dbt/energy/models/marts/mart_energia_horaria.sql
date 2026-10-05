
WITH base AS (

    SELECT
        id_subsistema,
        nome_subsistema,
        instante,

        carga_mwmed,
        geracao_hidraulica_mwmed,
        geracao_termica_mwmed,
        geracao_eolica_mwmed,
        geracao_solar_mwmed,
        intercambio_mwmed,

        data_ingestao,
        ano_referencia

    FROM {{ ref('int_balanco_carga_horaria') }}

),

metricas AS (

    SELECT
        *,

        CAST(instante AS DATE) AS data,
        DATE_PART('HOUR', instante) AS hora,

        (
            geracao_hidraulica_mwmed
            + geracao_termica_mwmed
            + geracao_eolica_mwmed
            + geracao_solar_mwmed
        ) AS geracao_total_mwmed

    FROM base

)

SELECT
    id_subsistema,
    nome_subsistema,
    instante,
    data,
    hora,

    carga_mwmed,

    geracao_hidraulica_mwmed,
    geracao_termica_mwmed,
    geracao_eolica_mwmed,
    geracao_solar_mwmed,
    geracao_total_mwmed,

    intercambio_mwmed,

    geracao_eolica_mwmed
        / NULLIF(geracao_total_mwmed, 0)
        * 100 AS participacao_eolica_pct,

    geracao_solar_mwmed
        / NULLIF(geracao_total_mwmed, 0)
        * 100 AS participacao_solar_pct,

    data_ingestao,
    ano_referencia

FROM metricas