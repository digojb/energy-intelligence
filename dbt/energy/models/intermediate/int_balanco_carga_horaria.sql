WITH carga AS (

    SELECT
        id_subsistema,
        nome_subsistema,
        instante,
        carga_mwmed,
        data_ingestao,
        ano_referencia

    FROM {{ ref('stg_curva_carga') }}

),

balanco AS (

    SELECT
        id_subsistema,
        instante,

        geracao_hidraulica_mwmed,
        geracao_termica_mwmed,
        geracao_eolica_mwmed,
        geracao_solar_mwmed,
        carga_mwmed AS carga_balanco_mwmed,
        intercambio_mwmed

    FROM {{ ref('stg_balanco_energia') }}

    WHERE id_subsistema <> 'SIN'

),

consolidado AS (

    SELECT
        c.id_subsistema,
        c.nome_subsistema,
        c.instante,

        c.carga_mwmed,

        b.geracao_hidraulica_mwmed,
        b.geracao_termica_mwmed,
        b.geracao_eolica_mwmed,
        b.geracao_solar_mwmed,

        b.carga_balanco_mwmed,
        b.intercambio_mwmed,

        c.data_ingestao,
        c.ano_referencia

    FROM carga c

    INNER JOIN balanco b
        ON c.id_subsistema = b.id_subsistema
        AND c.instante = b.instante

)

SELECT *
FROM consolidado