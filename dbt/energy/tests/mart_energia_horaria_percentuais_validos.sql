--Validação dos valores percentuais. Se forem maior que 100 ou menor que 0, o teste falha

SELECT *
FROM {{ ref('mart_energia_horaria') }}
WHERE participacao_eolica_pct < 0
   OR participacao_eolica_pct > 100
   OR participacao_solar_pct < 0
   OR participacao_solar_pct > 100