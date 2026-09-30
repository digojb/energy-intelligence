SELECT
    id_subsistema,
    instante,
    COUNT(*) AS quantidade
FROM {{ ref('stg_curva_carga') }}
GROUP BY
    id_subsistema,
    instante
HAVING COUNT(*) > 1