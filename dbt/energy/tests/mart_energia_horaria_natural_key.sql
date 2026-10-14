--Validação da chave natural. Se houver duplicidade, o teste falha.

SELECT
    id_subsistema,
    instante,
    COUNT(*) AS quantidade
FROM {{ ref('mart_energia_horaria') }}
GROUP BY
    id_subsistema,
    instante
HAVING COUNT(*) > 1