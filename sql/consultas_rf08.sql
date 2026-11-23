-- ============================================================
-- Consultas de evidencia do RF08 (relatorio por e-mail)
-- Banco: rpa. Rode no DBeaver depois dos testes de envio.
-- ============================================================

-- 1) Historico de envios: INFO = enviado, ERROR = falha no envio,
--    WARNING = e-mail nao configurado. Mostra tambem o status da execucao,
--    comprovando que o relatorio sai tanto em 'completed' quanto em 'failed'.
SELECT l.execucao_id,
       e.arquivo,
       e.status AS status_execucao,
       l.momento,
       l.nivel,
       l.mensagem
FROM log l
JOIN execucao e ON e.id = l.execucao_id
WHERE l.etapa = 'email'
ORDER BY l.momento DESC;

-- 2) Prova de que uma falha no envio nao derrubou o fluxo: para cada execucao
--    com ERROR de e-mail, a execucao terminou normalmente (finalizada_em e totais
--    preenchidos) e o log de finalizacao foi gravado antes da tentativa de envio.
SELECT e.id AS execucao,
       e.status,
       e.finalizada_em,
       e.total_sucesso,
       e.total_falha,
       l.momento AS momento_erro_email,
       l.mensagem AS erro_email
FROM execucao e
JOIN log l ON l.execucao_id = e.id AND l.etapa = 'email' AND l.nivel = 'ERROR'
ORDER BY e.id DESC;

-- 3) Figura "rf08_sql": historico de envios ligado ao status de cada execucao.
--    Os enderecos de e-mail sao mascarados (***) para a captura de tela.
--    A coluna execucao_finalizada mostra que a execucao ja estava encerrada
--    quando o envio foi tentado: um ERROR no e-mail nao afeta o processamento.
SELECT l.execucao_id AS execucao,
       e.status AS status_execucao,
       to_char(e.finalizada_em, 'DD/MM HH24:MI:SS') AS execucao_finalizada,
       to_char(l.momento, 'DD/MM HH24:MI:SS') AS registro_email,
       l.nivel,
       regexp_replace(split_part(l.mensagem, E'\n', 1), '[^ ,(]+@[^ ,)]+', '***', 'g') AS mensagem
FROM log l
JOIN execucao e ON e.id = l.execucao_id
WHERE l.etapa = 'email'
ORDER BY l.momento;
