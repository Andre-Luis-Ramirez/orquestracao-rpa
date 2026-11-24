# Orquestração de Processos Automatizados com RPA

Código-fonte do Trabalho de Conclusão de Curso **"Aplicação de Orquestração para Gerenciamento de Processos Automatizados Utilizando RPA"**.

A aplicação recebe uma planilha de colaboradores, valida os registros e usa um robô (Selenium) para cadastrá-los no formulário do [RPA Challenge](https://rpachallenge.com/). Toda a execução é orquestrada pelo **Prefect 3**:

- agendamento e monitoramento de um diretório de entrada (RF01);
- reexecução automática de etapas que falham (RF07);
- envio de relatório por e-mail ao final de cada execução (RF08).

Todos os eventos ficam registrados em um banco **PostgreSQL**, o que permite rastrear cada execução.

---

## Sumário

1. [Arquitetura](#arquitetura)
2. [Tecnologias](#tecnologias)
3. [Estrutura do projeto](#estrutura-do-projeto)
4. [Pré-requisitos](#pré-requisitos)
5. [Instalação](#instalação)
6. [Configuração do ambiente (.env)](#configuração-do-ambiente-env)
7. [Como executar](#como-executar)
8. [Cenários de teste de falha (RF07)](#cenários-de-teste-de-falha-rf07)
9. [Relatório por e-mail (RF08)](#relatório-por-e-mail-rf08)
10. [Banco de dados e consultas de evidência](#banco-de-dados-e-consultas-de-evidência)
11. [Formato da planilha de entrada](#formato-da-planilha-de-entrada)
12. [Solução de problemas](#solução-de-problemas)

---

## Arquitetura

```
 data/entrada/*.xlsx          Prefect Server (Docker, :4200)
        │                              ▲  registra runs, tasks e logs
        ▼                              │
 ┌──────────────────────────────────────────────────────────┐
 │ fluxo_monitoramento  (deployment agendado – RF01)        │
 │   └─ fluxo_cadastro_colaboradores  (subflow)             │
 │        1. registrar_inicio        → tabela execucao      │
 │        2. validar_e_persistir     → tabela colaborador   │
 │        3. processar_validos                              │
 │             └─ executar_rodada (10 registros, retry RF07)│
 │                  └─ robo.py (Selenium + Chrome)          │
 │        4. finalizar_execucao      → totais e status      │
 │        5. enviar_relatorio_email  → SMTP (RF08)          │
 └──────────────────────────────────────────────────────────┘
        │                                         │
        ▼                                         ▼
 processados/ ou erro/                PostgreSQL (Docker, :5433)
                                       bancos: prefect | rpa
```

O PostgreSQL do Docker hospeda **dois bancos**:

| Banco | Uso |
|---|---|
| `prefect` | Metadados internos do Prefect Server (flows, runs, deployments). |
| `rpa` | Dados da aplicação: tabelas `execucao`, `colaborador` e `log`. |

---

## Tecnologias

| Componente | Versão |
|---|---|
| Python | 3.12 |
| Prefect (orquestração) | 3.x |
| Selenium (RPA) | 4.x, com Google Chrome |
| pandas + openpyxl (leitura da planilha) | 2.x / 3.x |
| SQLAlchemy + psycopg2 (acesso ao banco) | 2.x |
| PostgreSQL | 16 (Docker) |
| Docker / Docker Compose | qualquer versão recente |

---

## Estrutura do projeto

```
rpa-orquestracao/
├── docker-compose.yml        # Prefect Server + PostgreSQL
├── init-db/
│   └── 01-create-rpa-db.sql  # cria o banco 'rpa' na primeira subida do container
├── schema.sql                # esquema do banco 'rpa' em SQL puro (documentação)
├── requirements.txt          # dependências Python
├── env.example               # modelo do arquivo .env
├── data/
│   ├── colaboradores.xlsx    # planilha de exemplo (56 registros: 50 válidos e 6 inválidos)
│   ├── planilha_invalida.xlsx# planilha com colunas erradas (teste de falha)
│   └── entrada/              # diretório monitorado (criado automaticamente)
│       ├── processados/      # planilhas processadas com sucesso
│       └── erro/             # planilhas cujo processamento falhou
├── sql/
│   ├── consultas_rf07.sql    # consultas de evidência do retry
│   └── consultas_rf08.sql    # consultas de evidência do e-mail
└── src/
    ├── database.py           # conexão com o banco e criação das tabelas
    ├── models.py             # modelos SQLAlchemy (execucao, colaborador, log)
    ├── validacao.py          # regras de validação da planilha
    ├── robo.py               # robô Selenium que preenche o RPA Challenge
    ├── monitoramento.py      # busca e movimentação de arquivos (RF01)
    ├── notificacao.py        # montagem e envio do relatório por e-mail (RF08)
    ├── fluxo.py              # flows e tasks do Prefect
    ├── servir.py             # publica o deployment agendado (RF01)
    └── testes_falha.py       # cenários reprodutíveis de falha (RF07)
```

---

## Pré-requisitos

- **Python 3.12** (ou 3.10+)
- **Docker Desktop** (ou Docker Engine + Docker Compose)
- **Google Chrome** instalado. O Selenium 4 baixa o ChromeDriver compatível sozinho (Selenium Manager).
- Acesso à internet, para usar o RPA Challenge e enviar e-mails.
- *(Opcional)* um cliente SQL, como o DBeaver, para consultar o banco.
- *(Opcional, para o RF08)* uma conta de e-mail com acesso SMTP, como Gmail com senha de app.

---

## Instalação

### 1. Clonar o repositório

```bash
git clone https://github.com/Andre-Luis-Ramirez/orquestracao-rpa.git
cd orquestracao-rpa
```

### 2. Subir a infraestrutura (PostgreSQL + Prefect Server)

```bash
docker compose up -d
```

Na primeira subida, o script `init-db/01-create-rpa-db.sql` cria o banco `rpa`. Confira se os dois containers estão no ar:

```bash
docker ps
# tcc_postgres        (porta 5433)
# tcc_prefect_server  (porta 4200)
```

A interface do Prefect fica em **http://localhost:4200**.

> O PostgreSQL é exposto na porta **5433** do computador, e não na 5432, para não conflitar com uma instalação local do PostgreSQL. Dentro da rede Docker ele continua na 5432.

### 3. Criar o ambiente virtual e instalar as dependências

**Windows (PowerShell):**

```powershell
python -m venv venv_tcc
.\venv_tcc\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Linux / macOS:**

```bash
python3 -m venv venv_tcc
source venv_tcc/bin/activate
pip install -r requirements.txt
```

### 4. Criar o arquivo `.env`

```powershell
copy env.example .env      # Windows
```

```bash
cp env.example .env        # Linux / macOS
```

Ajuste os valores conforme a seção [Configuração do ambiente](#configuração-do-ambiente-env).

### 5. Criar as tabelas do banco `rpa`

```bash
python -m src.database
# Tabelas criadas/verificadas com sucesso no banco 'rpa'.
```

O comando pode ser repetido sem risco: as tabelas só são criadas se ainda não existirem.

---

## Configuração do ambiente (.env)

As credenciais ficam **somente** no arquivo `.env`. Ele está no `.gitignore` e nunca é versionado.

| Variável | Padrão | Descrição |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg2://prefect:prefect@localhost:5433/rpa` | Conexão com o banco da aplicação. |
| `PREFECT_API_URL` | `http://localhost:4200/api` | API do Prefect Server. Sem ela, o Prefect roda em modo local temporário e nada aparece na UI. |
| `WATCH_DIR` | `./data/entrada` | Diretório monitorado (RF01). |
| `TARGET_URL` | `https://rpachallenge.com/` | Endereço do sistema alvo do robô. |
| `AGENDAMENTO_CRON` | *(vazio)* | Expressão cron do agendamento. Exemplo: `0 18 * * *` (todo dia às 18h). Quando preenchida, tem prioridade sobre o intervalo. |
| `AGENDAMENTO_FUSO` | `America/Sao_Paulo` | Fuso horário do cron. Sem ele, o cron seria interpretado em UTC. |
| `AGENDAMENTO_INTERVALO_SEGUNDOS` | `60` | Intervalo entre verificações, usado quando não há cron. |
| `MONITORAMENTO_HEADLESS` | `true` | Use `false` para ver o Chrome trabalhando durante o monitoramento. |
| `RETRY_DELAYS` | `10,30,60` | Esperas, em segundos, antes de cada uma das 3 novas tentativas de uma rodada que falhou. Aceita 1 ou 3 valores. |
| `SMTP_HOST` | `smtp.gmail.com` | Servidor SMTP (RF08). |
| `SMTP_PORT` | `587` | `587` usa STARTTLS; `465` usa SSL direto. |
| `SMTP_USUARIO` | — | Conta que envia o relatório. |
| `SMTP_SENHA` | — | Senha da conta. No Gmail, use uma **senha de app**, digitada sem espaços. |
| `EMAIL_DESTINATARIO` | — | Destinatário(s) do relatório, separados por vírgula. |

Se as variáveis de e-mail ficarem vazias, o sistema funciona normalmente. O relatório só não é enviado, e um `WARNING` é registrado na tabela `log`.

### Senha de app do Gmail

1. Ative a **verificação em duas etapas** na conta Google. Sem ela, a opção de senha de app não aparece.
2. Acesse <https://myaccount.google.com/apppasswords> e gere uma senha de app.
3. Copie os 16 caracteres, sem espaços, para `SMTP_SENHA`.

Contas corporativas ou de instituições de ensino podem ter esse recurso bloqueado pelo administrador. Nesse caso, use uma conta pessoal ou um serviço de teste de SMTP, como o Mailtrap.

---

## Como executar

Todos os comandos devem ser executados **na raiz do projeto**, com o ambiente virtual ativo e os containers no ar.

### Execução manual (uma planilha)

Coloque a planilha no diretório de entrada e execute o fluxo:

```bash
copy data\colaboradores.xlsx data\entrada\     # Windows
# cp data/colaboradores.xlsx data/entrada/     # Linux / macOS

python -m src.fluxo
```

Esse modo processa `data/entrada/colaboradores.xlsx` com o Chrome **visível** (`headless=False`), para acompanhar o robô. Ao final, o resumo aparece no terminal, e a execução pode ser vista na UI do Prefect (**Runs**) e na tabela `execucao`.

### Execução agendada com monitoramento do diretório (RF01)

```bash
python -m src.servir
```

O comando publica o deployment **`monitoramento-diretorio`** no Prefect Server e fica em execução, atendendo os runs agendados. Para parar, use `Ctrl+C`. A cada ciclo do agendamento, o fluxo:

1. procura a planilha `.xlsx` mais antiga em `data/entrada/`, ignorando subpastas e arquivos temporários do Excel (`~$...`);
2. se encontrar uma, executa o fluxo de cadastro como **subflow**;
3. move a planilha para `processados/` (sucesso) ou `erro/` (falha), acrescentando data e hora ao nome. Assim, o mesmo arquivo nunca é processado duas vezes.

Se não houver arquivo novo, o run termina rapidamente e registra "Nenhum arquivo novo no diretório monitorado".

Para testar, com o `servir` rodando, copie uma planilha para `data/entrada/` e aguarde o próximo ciclo (60 s no padrão). O deployment aparece na UI em **Deployments → monitoramento-diretorio**.

> O deployment usa `limit=1`: só um run é executado por vez. Um processamento longo nunca é sobreposto pelo ciclo seguinte.

---

## Cenários de teste de falha (RF07)

Cada rodada do robô (10 registros) é uma task do Prefect com **3 novas tentativas automáticas**, ou seja, até 4 tentativas no total. As esperas entre as tentativas vêm de `RETRY_DELAYS`. Se uma rodada esgotar as tentativas, os colaboradores dela são marcados como `falha`, um `ERROR` é registrado e o fluxo **segue para as próximas rodadas**.

Para reproduzir esses comportamentos:

> **Antes de começar:** pare o `python -m src.servir`. Se ele estiver ativo, o monitoramento pode mover a planilha de `data/entrada/` no meio do teste.

```bash
# A rodada 2 falha nas 2 primeiras tentativas e é recuperada na 3ª
python -m src.testes_falha recuperada

# A rodada 2 falha nas 4 tentativas; as rodadas 3, 4 e 5 seguem normalmente
python -m src.testes_falha definitiva

# Falha real: o robô acessa um endereço inexistente e todas as rodadas falham
python -m src.testes_falha url-invalida
```

| Opção | Padrão | Descrição |
|---|---|---|
| `--planilha CAMINHO` | `data/entrada/colaboradores.xlsx` | Planilha usada no teste. |
| `--rodada N` | `2` | Rodada em que a falha é simulada. |
| `--esperas "3,3,3"` | `3,3,3` | Esperas curtas para o teste não demorar. |
| `--ver-chrome` | desligado | Mostra o navegador durante o teste. |

Ao final, o terminal mostra o `execucao_id` do teste, que pode ser usado nas consultas de `sql/consultas_rf07.sql`. As falhas simuladas aparecem na tabela `log` (etapa `teste`), para não serem confundidas com erros reais.

---

## Relatório por e-mail (RF08)

Ao final de **toda** execução, tanto as concluídas (`completed`) quanto as que falharam (`failed`), o fluxo envia um relatório com:

- identificação, status, início, fim e duração;
- totais: registros na planilha, válidos, cadastrados com sucesso e falhas;
- registros reprovados na validação, com o motivo;
- falhas de cadastro, com o erro.

Uma falha no envio, como senha errada ou falta de internet, **não derruba o fluxo**. O erro é registrado na tabela `log` com nível `ERROR` e etapa `email`.

Comandos de apoio:

```bash
python -m src.notificacao teste          # envia um e-mail simples para validar o SMTP
python -m src.notificacao previa 7       # mostra o relatório da execução 7 no terminal, sem enviar
python -m src.notificacao execucao 7     # reenvia o relatório da execução 7 (registra na tabela log)
```

---

## Banco de dados e consultas de evidência

### Conexão (DBeaver ou outro cliente)

| Campo | Valor |
|---|---|
| Host | `localhost` |
| Porta | `5433` |
| Banco | `rpa` |
| Usuário | `prefect` |
| Senha | `prefect` |

> As credenciais `prefect/prefect` são as do ambiente de desenvolvimento definido no `docker-compose.yml`. Em um ambiente real, devem ser trocadas.

### Tabelas

| Tabela | Conteúdo |
|---|---|
| `execucao` | Uma linha por execução: arquivo, status (`running`, `completed` ou `failed`), horários e totais. |
| `colaborador` | Cada registro da planilha: os 7 campos, o resultado da validação (`valido`, `motivo_invalido`) e do cadastro (`status_cadastro`, `detalhe_erro`). |
| `log` | Eventos de cada etapa: `execucao_id`, `momento`, `nivel` (`INFO`, `WARNING` ou `ERROR`), `etapa` e `mensagem`. |

O esquema completo está em [`schema.sql`](schema.sql).

### Consultas prontas

- [`sql/consultas_rf07.sql`](sql/consultas_rf07.sql): linha do tempo das tentativas, resultado por colaborador e comparativo entre os cenários de falha.
- [`sql/consultas_rf08.sql`](sql/consultas_rf08.sql): histórico de envios de e-mail e prova de que uma falha no envio não afeta o processamento.

Exemplo, os eventos da última execução:

```sql
SELECT momento, nivel, etapa, mensagem
FROM log
WHERE execucao_id = (SELECT max(id) FROM execucao)
ORDER BY momento, id;
```

---

## Formato da planilha de entrada

Arquivo `.xlsx` com as 7 colunas abaixo (os nomes precisam ser exatamente estes):

| Coluna | Exemplo |
|---|---|
| `first_name` | John |
| `last_name` | Smith |
| `company_name` | IT Solutions |
| `role_in_company` | Analyst |
| `address` | 98 North Road |
| `email` | jsmith@itsolutions.co.uk |
| `phone_number` | 40716543298 |

Regras de validação, aplicadas antes do robô:

1. todos os 7 campos preenchidos;
2. e-mail em formato válido (`nome@dominio.ext`);
3. telefone apenas com dígitos, com no mínimo 8.

Registros inválidos não vão para o robô. Eles ficam gravados com `valido = false` e o motivo, e aparecem no relatório por e-mail. Se faltar alguma coluna, a execução inteira termina como `failed`. É o caso de `data/planilha_invalida.xlsx`.

O robô processa os registros em **rodadas de 10**, porque cada rodada do RPA Challenge tem exatamente 10 formulários. Os campos são localizados pelo atributo `ng-reflect-name`, já que o site troca a posição deles a cada envio.

---

## Solução de problemas

| Sintoma | Causa provável e solução |
|---|---|
| `password authentication failed for user "prefect"` | Outro PostgreSQL local está ocupando a porta. Confirme que `DATABASE_URL` usa a porta **5433**. |
| Nenhuma tabela aparece no banco `rpa` | Rode `python -m src.database`. |
| `ModuleNotFoundError: No module named 'src'` | Execute os comandos na raiz do projeto, como módulo: `python -m src.fluxo`, e não `python src/fluxo.py`. |
| Runs não aparecem na UI do Prefect | Confira `PREFECT_API_URL` no `.env` e se o container `tcc_prefect_server` está no ar. |
| Comandos travam sem saída / `docker ps` não responde | O Docker Desktop travou. Reinicie-o e rode `docker compose up -d` novamente. |
| Robô falha com `ERR_NAME_NOT_RESOLVED` ou `ERR_CONNECTION_TIMED_OUT` | Falta de conexão com a internet ou instabilidade do RPA Challenge. O retry tenta novamente; se persistir, verifique a rede. |
| Relatório não chega e o log mostra `WARNING` na etapa `email` | Variáveis SMTP vazias no `.env`. |
| Relatório não chega e o log mostra `ERROR` na etapa `email` | Credencial inválida ou SMTP bloqueado. Valide com `python -m src.notificacao teste`. |
| A planilha some de `data/entrada/` | O `servir` está ativo e a processou. Ela está em `processados/` ou `erro/`. |

---

## Autor

André Luís Ramirez, Trabalho de Conclusão de Curso em Tecnologia em Sistemas para Internet.
