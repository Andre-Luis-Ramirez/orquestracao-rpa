-- ============================================================
-- schema.sql
-- Esquema do banco da aplicacao (banco 'rpa')
-- Equivalente em SQL puro aos models SQLAlchemy (src/models.py)
-- Util para documentacao e visualizacao na monografia.
-- ============================================================

-- Tabela de execucoes: uma linha por execucao do workflow
CREATE TABLE execucao (
    id               SERIAL PRIMARY KEY,
    arquivo          VARCHAR(255) NOT NULL,
    status           VARCHAR(20)  NOT NULL DEFAULT 'running',  -- running | completed | failed
    iniciada_em      TIMESTAMP    NOT NULL DEFAULT NOW(),
    finalizada_em    TIMESTAMP,
    total_registros  INTEGER DEFAULT 0,
    total_validos    INTEGER DEFAULT 0,
    total_sucesso    INTEGER DEFAULT 0,
    total_falha      INTEGER DEFAULT 0
);

-- Tabela de colaboradores: registros da planilha, vinculados a uma execucao
CREATE TABLE colaborador (
    id               SERIAL PRIMARY KEY,
    execucao_id      INTEGER NOT NULL REFERENCES execucao(id) ON DELETE CASCADE,
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    company_name     VARCHAR(150),
    role_in_company  VARCHAR(150),
    address          VARCHAR(255),
    email            VARCHAR(150),
    phone_number     VARCHAR(50),
    valido           BOOLEAN DEFAULT TRUE,
    motivo_invalido  VARCHAR(255),
    status_cadastro  VARCHAR(20),   -- sucesso | falha | nao_processado
    detalhe_erro     TEXT
);

-- Tabela de logs: eventos detalhados de cada etapa, vinculados a uma execucao
CREATE TABLE log (
    id               SERIAL PRIMARY KEY,
    execucao_id      INTEGER NOT NULL REFERENCES execucao(id) ON DELETE CASCADE,
    momento          TIMESTAMP NOT NULL DEFAULT NOW(),
    nivel            VARCHAR(10) NOT NULL DEFAULT 'INFO',  -- INFO | WARNING | ERROR
    etapa            VARCHAR(50),
    mensagem         TEXT NOT NULL
);

-- Indices uteis para consultas de rastreabilidade
CREATE INDEX idx_colaborador_execucao ON colaborador(execucao_id);
CREATE INDEX idx_log_execucao ON log(execucao_id);
