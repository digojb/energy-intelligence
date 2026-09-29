WITH source AS (

    SELECT
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        DIN_INSTANTE,
        VAL_CARGAENERGIAHOMWMED,
        DATA_INGESTAO,
        FONTE,
        ANO_REFERENCIA

    FROM {{ source('raw', 'CURVA_CARGA') }}

),

renamed AS (

    SELECT
        ID_SUBSISTEMA AS id_subsistema,
        NOM_SUBSISTEMA AS nome_subsistema,
        DIN_INSTANTE AS instante,
        VAL_CARGAENERGIAHOMWMED AS carga_mwmed,
        DATA_INGESTAO AS data_ingestao,
        FONTE AS fonte,
        ANO_REFERENCIA AS ano_referencia

    FROM source

)

SELECT *
FROM renamed