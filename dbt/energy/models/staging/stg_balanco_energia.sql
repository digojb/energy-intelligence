WITH source AS (

    SELECT
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

    FROM {{ source('raw', 'BALANCO_ENERGIA') }}

),

renamed AS (

    SELECT
        ID_SUBSISTEMA AS id_subsistema,
        NOM_SUBSISTEMA AS nome_subsistema,
        DIN_INSTANTE AS instante,

        VAL_GERHIDRAULICA AS geracao_hidraulica_mwmed,
        VAL_GERTERMICA AS geracao_termica_mwmed,
        VAL_GEREOLICA AS geracao_eolica_mwmed,
        VAL_GERSOLAR AS geracao_solar_mwmed,

        VAL_CARGA AS carga_mwmed,
        VAL_INTERCAMBIO AS intercambio_mwmed,

        DATA_INGESTAO AS data_ingestao,
        FONTE AS fonte,
        ANO_REFERENCIA AS ano_referencia

    FROM source

)

SELECT *
FROM renamed