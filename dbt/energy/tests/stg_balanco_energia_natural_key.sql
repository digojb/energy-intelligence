SELECT
    id_subsistema,
    instante,
    COUNT(*) AS quantidade
FROM {{ ref('stg_balanco_energia') }}
GROUP BY
    id_subsistema,
    instante
HAVING COUNT(*) > 1