-- ============================================================
-- Consultas de evidencia do RF07 (reexecucao automatica em caso de falha)
-- Banco: rpa. Rode no DBeaver depois de "python -m src.testes_falha <cenario>".
-- As consultas 1 e 2 usam a execucao mais recente (SELECT max(id) FROM execucao).
-- Para ver um teste anterior, troque essa subconsulta pelo id impresso no fim do teste.
-- ============================================================

-- 1) Linha do tempo das tentativas de cada rodada: inicio de cada tentativa,
--    falhas (WARNING), recuperacao (INFO) e esgotamento (ERROR).
SELECT l.momento,
       l.nivel,
       l.etapa,
       l.mensagem
FROM log l
WHERE l.execucao_id = (SELECT max(id) FROM execucao)
  AND (l.etapa IN ('teste', 'retry')
       OR (l.etapa = 'rpa' AND (l.mensagem LIKE '%tentativa%' OR l.nivel = 'ERROR')))
ORDER BY l.momento, l.id;

-- 2) Resultado por colaborador: na falha definitiva, os 10 da rodada afetada
--    ficam com status 'falha' e o detalhe do erro; os demais seguem 'sucesso'.
SELECT status_cadastro,
       count(*) AS quantidade,
       min(detalhe_erro) AS exemplo_de_erro
FROM colaborador
WHERE execucao_id = (SELECT max(id) FROM execucao)
  AND valido
GROUP BY status_cadastro
ORDER BY status_cadastro;

-- 3) Comparativo entre os testes: quantas tentativas falharam e quantas
--    rodadas foram recuperadas ou perdidas em cada execucao.
SELECT e.id AS execucao,
       e.iniciada_em,
       e.status,
       e.total_sucesso,
       e.total_falha,
       count(*) FILTER (WHERE l.etapa = 'retry' AND l.nivel = 'WARNING') AS tentativas_com_falha,
       count(*) FILTER (WHERE l.etapa = 'retry' AND l.nivel = 'INFO')    AS rodadas_recuperadas,
       count(*) FILTER (WHERE l.etapa = 'rpa'   AND l.nivel = 'ERROR')   AS rodadas_perdidas
FROM execucao e
LEFT JOIN log l ON l.execucao_id = e.id
GROUP BY e.id
ORDER BY e.id DESC;

-- 4) Figura "rf07_sql": linha do tempo de UMA rodada reexecutada (rodada 2 do
--    cenario 'recuperada' mais recente). A ultima coluna mostra os segundos
--    desde o evento anterior: o intervalo entre cada falha (WARNING) e o inicio
--    da tentativa seguinte corresponde a espera configurada em RETRY_DELAYS.
WITH alvo AS (
    SELECT max(execucao_id) AS id
    FROM log
    WHERE etapa = 'inicio' AND mensagem LIKE '%teste RF07 (recuperada)%'
)
SELECT l.execucao_id AS execucao,
       to_char(l.momento, 'HH24:MI:SS.MS') AS horario,
       l.nivel,
       l.etapa,
       split_part(l.mensagem, E'\n', 1) AS evento,
       round(extract(epoch FROM l.momento - lag(l.momento) OVER (ORDER BY l.momento, l.id))::numeric, 1)
           AS segundos_desde_evento_anterior
FROM log l
JOIN alvo ON l.execucao_id = alvo.id
WHERE l.mensagem LIKE 'Rodada 2%'
ORDER BY l.momento, l.id;

-- 5) Quadro comparativo: execucao regular x cenarios de falha.
--    Ajuste os ids se quiser comparar outras execucoes.
SELECT e.id AS execucao,
       substring(min(l.mensagem) FILTER (WHERE l.etapa = 'inicio') from 'origem: (.*)\)\.$') AS cenario,
       e.total_validos AS validos,
       e.total_sucesso AS sucesso,
       e.total_falha AS falha,
       count(*) FILTER (WHERE l.etapa = 'retry' AND l.nivel = 'WARNING') AS tentativas_com_falha,
       count(*) FILTER (WHERE l.etapa = 'retry' AND l.nivel = 'INFO')    AS rodadas_recuperadas,
       count(*) FILTER (WHERE l.etapa = 'rpa'   AND l.nivel = 'ERROR')   AS rodadas_perdidas,
       e.status,
       to_char(e.finalizada_em - e.iniciada_em, 'MI"min "SS"s"') AS duracao
FROM execucao e
JOIN log l ON l.execucao_id = e.id
WHERE e.id IN (8, 10, 7)
GROUP BY e.id
ORDER BY array_position(ARRAY[8, 10, 7], e.id);
