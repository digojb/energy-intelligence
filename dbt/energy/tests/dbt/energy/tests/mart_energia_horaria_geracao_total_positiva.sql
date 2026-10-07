--Validação da geração total. Se for menor que 0, o teste falha.

SELECT *
FROM {{ ref('mart_energia_horaria') }}
WHERE geracao_total_mwmed < 0