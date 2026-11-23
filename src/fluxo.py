"""
Camada de orquestracao do processo de cadastro de colaboradores (Prefect 3).

Este modulo amarra os demais componentes da solucao:
  - src/validacao.py : leitura e validacao da planilha
  - src/robo.py      : robo RPA que preenche o formulario do RPA Challenge
  - src/models.py    : tabelas Execucao, Colaborador e Log (rastreabilidade)

Estrutura do flow (cada etapa e uma task do Prefect):

  1. registrar_inicio       -> cria a Execucao com status "running"
  2. validar_e_persistir    -> valida a planilha e grava todos os colaboradores
  3. processar_validos      -> divide os validos em rodadas de 10 e, para cada
                               rodada, chama a task executar_rodada (com retry)
  4. finalizar_execucao     -> calcula os totais e encerra a Execucao
  5. enviar_relatorio_email -> envia o relatorio da execucao por e-mail (RF08)

Decisao importante: o retry fica na task de UMA rodada (executar_rodada), e
nao na task que percorre todas as rodadas. Assim, se a rodada 3 falhar, so ela
e repetida; as rodadas 1 e 2, que ja deram certo, nao sao reenviadas ao site.
Se uma rodada esgotar todas as tentativas, a falha e registrada e o fluxo
continua com a proxima rodada (tratamento isolado de falhas).

Ha ainda um segundo flow, fluxo_monitoramento (RF01), que e executado de
forma agendada (ver src/servir.py): ele procura uma planilha nova no
diretorio monitorado e, se encontrar, chama fluxo_cadastro_colaboradores
como subflow.
"""
import os
import sys
from datetime import datetime
from pathlib import Path

# Quando o arquivo e executado diretamente (python src/fluxo.py, ou pelo botao
# de debug do VS Code), o Python nao enxerga a raiz do projeto e os imports
# "from src.xxx" falham. Aqui a raiz e adicionada ao caminho de busca e vira o
# diretorio de trabalho, para que o .env e a planilha sejam encontrados.
RAIZ_PROJETO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROJETO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROJETO))
os.chdir(RAIZ_PROJETO)

from dotenv import load_dotenv  # noqa: E402

# Carrega o .env ANTES de importar o Prefect, para que a variavel
# PREFECT_API_URL ja esteja definida e o flow seja registrado no servidor
# (UI em http://localhost:4200).
load_dotenv()

from prefect import flow, task, get_run_logger  # noqa: E402
from prefect.cache_policies import NO_CACHE  # noqa: E402
from prefect.runtime import task_run  # noqa: E402
from sqlalchemy import select, func  # noqa: E402

# O banco e acessado como "database.SessionLocal()" (pelo modulo), e nao
# importando SessionLocal diretamente. Motivo: ao executar um deployment
# (src/servir.py), o Prefect serializa o flow com cloudpickle para roda-lo em
# um subprocesso. Um objeto global como SessionLocal seria copiado junto, e o
# pool de conexoes do SQLAlchemy contem travas (locks) que nao podem ser
# serializadas. Ja um modulo e serializado apenas pelo nome e reimportado.
from src import database  # noqa: E402
from src.database import init_db  # noqa: E402
from src.models import Execucao, Colaborador, Log  # noqa: E402
from src.validacao import validar_planilha, COLUNAS_ESPERADAS  # noqa: E402
from src.robo import processar_rodada, TAMANHO_RODADA  # noqa: E402
from src.monitoramento import (  # noqa: E402
    buscar_arquivo_novo, mover_para_processados, mover_para_erro,
)
from src.notificacao import (  # noqa: E402
    enviar_email, montar_relatorio, ConfiguracaoEmailAusente,
)

# Politica de retry de cada rodada do robo (RF07):
# ate 3 novas tentativas, com espera crescente entre elas (padrao 10s, 30s e
# 60s), para dar tempo de o site ou a rede se recuperarem.
RETRIES_RODADA = 3


def ler_esperas_retry() -> list[int]:
    """
    Le do .env os tempos de espera entre as tentativas (RETRY_DELAYS).
    Formato: numeros em segundos separados por virgula, ex. "10,30,60".
    Um unico valor (ex. "2") vale para todas as tentativas. Valores menores
    servem para deixar os testes de falha mais rapidos, sem mudar o padrao.
    """
    texto = os.getenv("RETRY_DELAYS", "10,30,60")
    try:
        esperas = [int(valor) for valor in texto.split(",") if valor.strip()]
    except ValueError:
        raise ValueError(f"RETRY_DELAYS invalido: '{texto}'. Use numeros separados "
                         f"por virgula, ex. 10,30,60.") from None
    if len(esperas) == 1:
        esperas = esperas * RETRIES_RODADA
    if len(esperas) != RETRIES_RODADA:
        raise ValueError(f"RETRY_DELAYS deve ter 1 ou {RETRIES_RODADA} valores; "
                         f"recebido: '{texto}'.")
    return esperas


ESPERAS_RETRY = ler_esperas_retry()

# Mapeia o nivel gravado na tabela Log para o metodo do logger do Prefect
METODOS_LOGGER = {"INFO": "info", "WARNING": "warning", "ERROR": "error"}


# ---------------------------------------------------------------------------
# Funcoes auxiliares (nao sao tasks: sao chamadas de dentro das tasks)
# ---------------------------------------------------------------------------

def registrar_log(execucao_id: int, nivel: str, etapa: str, mensagem: str) -> None:
    """
    Registra um evento em dois lugares ao mesmo tempo:
      - no logger do Prefect (aparece na UI, junto ao run do flow/task);
      - na tabela Log do banco 'rpa' (rastreabilidade permanente).
    """
    logger = get_run_logger()
    getattr(logger, METODOS_LOGGER.get(nivel, "info"))(f"[{etapa}] {mensagem}")

    with database.SessionLocal() as sessao:
        sessao.add(Log(
            execucao_id=execucao_id,
            nivel=nivel,
            etapa=etapa,
            mensagem=mensagem,
        ))
        sessao.commit()


def salvar_resultados(resultados: list[dict]) -> None:
    """
    Atualiza no banco o status_cadastro e o detalhe_erro de cada colaborador.
    Cada resultado carrega o 'colaborador_id', gravado na etapa de validacao,
    o que permite localizar a linha exata a ser atualizada.
    """
    with database.SessionLocal() as sessao:
        for resultado in resultados:
            colaborador = sessao.get(Colaborador, resultado["colaborador_id"])
            colaborador.status_cadastro = resultado["status_cadastro"]
            colaborador.detalhe_erro = resultado.get("detalhe_erro")
        sessao.commit()


# ---------------------------------------------------------------------------
# Tasks
# Todas usam cache_policy=NO_CACHE: elas gravam no banco, entao devem rodar
# sempre, e nao ser puladas por um resultado guardado em cache.
# ---------------------------------------------------------------------------

@task(name="Registrar inicio da execucao", cache_policy=NO_CACHE)
def registrar_inicio(caminho_planilha: str, origem: str = "manual") -> int:
    """
    Etapa 1: cria a Execucao com status 'running' e devolve o seu id.
    'origem' indica quem disparou a execucao: "manual" (python -m src.fluxo)
    ou "diretorio monitorado" (fluxo_monitoramento agendado).
    """
    with database.SessionLocal() as sessao:
        execucao = Execucao(
            arquivo=os.path.basename(caminho_planilha),
            status="running",
            iniciada_em=datetime.now(),
        )
        sessao.add(execucao)
        sessao.commit()
        execucao_id = execucao.id  # id gerado pelo banco apos o commit

    registrar_log(execucao_id, "INFO", "inicio",
                  f"Execucao {execucao_id} iniciada para o arquivo '{caminho_planilha}' "
                  f"(origem: {origem}).")
    return execucao_id


@task(name="Validar e persistir colaboradores", cache_policy=NO_CACHE)
def validar_e_persistir(execucao_id: int, caminho_planilha: str) -> list[dict]:
    """
    Etapa 2: valida a planilha e grava TODOS os colaboradores no banco
    (validos e invalidos). Devolve apenas os validos, ja com o 'colaborador_id'.
    """
    registros = validar_planilha(caminho_planilha)

    validos = []
    with database.SessionLocal() as sessao:
        pares = []  # (registro, colaborador) para recuperar os ids depois
        for registro in registros:
            colaborador = Colaborador(
                execucao_id=execucao_id,
                **{campo: registro[campo] for campo in COLUNAS_ESPERADAS},
                valido=registro["valido"],
                motivo_invalido=registro["motivo_invalido"] or None,
                # Invalidos nunca vao para o robo; validos aguardam o processamento
                status_cadastro=None if registro["valido"] else "nao_processado",
            )
            sessao.add(colaborador)
            pares.append((registro, colaborador))

        # flush envia os INSERTs ao banco e preenche os ids, sem encerrar a transacao
        sessao.flush()

        for registro, colaborador in pares:
            if registro["valido"]:
                dados = {campo: registro[campo] for campo in COLUNAS_ESPERADAS}
                dados["colaborador_id"] = colaborador.id
                validos.append(dados)

        sessao.commit()

    # Um log por registro reprovado, com o motivo (rastreabilidade)
    for registro in registros:
        if not registro["valido"]:
            nome = f"{registro['first_name']} {registro['last_name']}".strip() or "(sem nome)"
            registrar_log(execucao_id, "WARNING", "validacao",
                          f"Registro reprovado - {nome}: {registro['motivo_invalido']}")

    registrar_log(execucao_id, "INFO", "validacao",
                  f"Planilha validada: {len(registros)} registro(s), "
                  f"{len(validos)} valido(s), {len(registros) - len(validos)} invalido(s).")
    return validos


@task(
    name="Executar rodada do robo",
    retries=RETRIES_RODADA,
    retry_delay_seconds=ESPERAS_RETRY,
    cache_policy=NO_CACHE,
)
def executar_rodada(execucao_id: int, numero_rodada: int, registros: list[dict],
                    headless: bool, target_url: str,
                    simular_falha_rodada: int | None = None,
                    tentativas_com_falha: int = 0) -> dict:
    """
    Executa UMA rodada (ate 10 registros) no RPA Challenge.

    Se o robo lancar uma excecao (ex.: site fora do ar, botao Start nao
    encontrado, Chrome travou), o Prefect repete esta task automaticamente,
    conforme RETRIES_RODADA e ESPERAS_RETRY (RF07).

    Observacao: erros em um formulario especifico sao tratados dentro do
    proprio robo (o registro volta com status 'falha') e nao disparam retry.

    Parametros de teste (RF07), usados apenas para gerar evidencias:
        simular_falha_rodada : numero da rodada em que a falha sera simulada
        tentativas_com_falha : quantas tentativas dessa rodada devem falhar.
                               Ex.: 2 -> falha na 1a e 2a, recupera na 3a;
                               4 -> todas as tentativas falham (falha definitiva).
    """
    tentativa = task_run.run_count  # 1 na primeira execucao, 2 no primeiro retry...
    total_tentativas = RETRIES_RODADA + 1
    registrar_log(execucao_id, "INFO", "rpa",
                  f"Rodada {numero_rodada}: tentativa {tentativa} de {total_tentativas} "
                  f"com {len(registros)} registro(s).")

    try:
        if simular_falha_rodada == numero_rodada and tentativa <= tentativas_com_falha:
            # Falha proposital, lancada antes de abrir o Chrome, para testar o retry
            raise RuntimeError(
                f"Falha simulada na rodada {numero_rodada}, tentativa {tentativa}")

        saida = processar_rodada(registros, headless=headless, target_url=target_url)

    except Exception as erro:
        # Registra a tentativa que falhou. Em seguida a excecao e relancada:
        # e ela que faz o Prefect agendar a proxima tentativa.
        if tentativa <= RETRIES_RODADA:
            espera = ESPERAS_RETRY[tentativa - 1]
            proximo_passo = f"Nova tentativa em {espera} s."
        else:
            proximo_passo = "Tentativas esgotadas."
        registrar_log(execucao_id, "WARNING", "retry",
                      f"Rodada {numero_rodada}, tentativa {tentativa} de {total_tentativas} "
                      f"falhou: {erro}. {proximo_passo}")
        raise

    if tentativa > 1:
        registrar_log(execucao_id, "INFO", "retry",
                      f"Rodada {numero_rodada} recuperada na tentativa {tentativa} "
                      f"de {total_tentativas}.")
    return saida


@task(name="Processar colaboradores no robo", cache_policy=NO_CACHE)
def processar_validos(execucao_id: int, validos: list[dict],
                      headless: bool, target_url: str,
                      simular_falha_rodada: int | None = None,
                      tentativas_com_falha: int = 0) -> None:
    """
    Etapa 3: divide os validos em grupos de TAMANHO_RODADA (10) e processa
    cada grupo com a task executar_rodada. Grava o resultado de cada
    colaborador e os logs de cada rodada.
    """
    if not validos:
        registrar_log(execucao_id, "WARNING", "rpa",
                      "Nenhum registro valido para processar. Robo nao executado.")
        return

    # Fatia a lista: [0:10], [10:20], [20:30], ...
    grupos = [validos[i:i + TAMANHO_RODADA] for i in range(0, len(validos), TAMANHO_RODADA)]
    registrar_log(execucao_id, "INFO", "rpa",
                  f"{len(validos)} registro(s) valido(s) divididos em {len(grupos)} rodada(s).")

    for numero, grupo in enumerate(grupos, start=1):
        try:
            saida = executar_rodada(execucao_id, numero, grupo, headless, target_url,
                                    simular_falha_rodada, tentativas_com_falha)
            resultados = saida["resultados"]
            registrar_log(execucao_id, "INFO", "rpa",
                          f"Rodada {numero} concluida. Mensagem do site: {saida['mensagem_site']}")
        except Exception as erro:
            # A rodada falhou mesmo apos todos os retries: registra e segue em frente.
            registrar_log(execucao_id, "ERROR", "rpa",
                          f"Rodada {numero} falhou apos {RETRIES_RODADA + 1} tentativa(s): {erro}")
            resultados = [
                {**registro, "status_cadastro": "falha",
                 "detalhe_erro": f"Rodada {numero} falhou: {erro}"}
                for registro in grupo
            ]

        salvar_resultados(resultados)

        # Um log por colaborador que falhou, com o detalhe do erro
        for resultado in resultados:
            if resultado["status_cadastro"] == "falha":
                nome = f"{resultado['first_name']} {resultado['last_name']}"
                registrar_log(execucao_id, "WARNING", "persistencia",
                              f"Cadastro falhou - {nome}: {resultado.get('detalhe_erro')}")

        sucessos = sum(1 for r in resultados if r["status_cadastro"] == "sucesso")
        registrar_log(execucao_id, "INFO", "persistencia",
                      f"Rodada {numero}: {sucessos} sucesso(s), "
                      f"{len(resultados) - sucessos} falha(s) gravados no banco.")


@task(name="Finalizar execucao", cache_policy=NO_CACHE)
def finalizar_execucao(execucao_id: int, status: str = "completed") -> dict:
    """
    Etapa 4: calcula os totais e encerra a Execucao.

    Os totais sao contados direto na tabela Colaborador (fonte da verdade),
    garantindo que a Execucao reflita exatamente o que foi gravado.
    """
    with database.SessionLocal() as sessao:
        base = (select(func.count())
                .select_from(Colaborador)
                .where(Colaborador.execucao_id == execucao_id))

        totais = {
            "total_registros": sessao.scalar(base),
            "total_validos": sessao.scalar(base.where(Colaborador.valido.is_(True))),
            "total_sucesso": sessao.scalar(base.where(Colaborador.status_cadastro == "sucesso")),
            "total_falha": sessao.scalar(base.where(Colaborador.status_cadastro == "falha")),
        }

        execucao = sessao.get(Execucao, execucao_id)
        execucao.status = status
        execucao.finalizada_em = datetime.now()
        for campo, valor in totais.items():
            setattr(execucao, campo, valor)
        sessao.commit()

    nivel = "INFO" if status == "completed" else "ERROR"
    registrar_log(execucao_id, nivel, "finalizacao",
                  f"Execucao {execucao_id} finalizada com status '{status}'. "
                  f"Registros: {totais['total_registros']}, validos: {totais['total_validos']}, "
                  f"sucesso: {totais['total_sucesso']}, falha: {totais['total_falha']}.")

    return {"execucao_id": execucao_id, "status": status, **totais}


@task(name="Enviar relatorio por e-mail", cache_policy=NO_CACHE)
def enviar_relatorio_email(execucao_id: int) -> bool:
    """
    Etapa 5 (RF08): envia por e-mail o relatorio da execucao.

    O envio e um passo "melhor esforco": se falhar (credencial errada, sem
    internet, servidor fora do ar), o erro e registrado na tabela Log e o
    fluxo segue normalmente. O processamento dos colaboradores ja terminou e
    nao deve ser marcado como falho por causa da notificacao.

    Devolve True se o e-mail foi enviado e False caso contrario.
    """
    try:
        assunto, corpo_texto, corpo_html = montar_relatorio(execucao_id)
        destinatarios = enviar_email(assunto, corpo_texto, corpo_html)
    except ConfiguracaoEmailAusente as erro:
        # Nao e falha de envio: o e-mail simplesmente ainda nao foi configurado
        registrar_log(execucao_id, "WARNING", "email", f"Relatorio nao enviado: {erro}.")
        return False
    except Exception as erro:
        registrar_log(execucao_id, "ERROR", "email",
                      f"Falha ao enviar o relatorio por e-mail: "
                      f"{type(erro).__name__}: {erro}")
        return False

    registrar_log(execucao_id, "INFO", "email",
                  f"Relatorio enviado por e-mail para {', '.join(destinatarios)} "
                  f"(assunto: '{assunto}').")
    return True


# ---------------------------------------------------------------------------
# Flow principal
# ---------------------------------------------------------------------------

@flow(name="Orquestracao RPA - Cadastro de Colaboradores")
def fluxo_cadastro_colaboradores(caminho_planilha: str, headless: bool = True,
                                 target_url: str | None = None,
                                 origem: str = "manual",
                                 simular_falha_rodada: int | None = None,
                                 tentativas_com_falha: int = 0) -> dict:
    """
    Orquestra o processo completo, na ordem:
    inicio -> validacao/persistencia -> robo (por rodadas) -> finalizacao.

    Parametros:
        caminho_planilha     : caminho do .xlsx com os colaboradores
        headless             : False para ver o Chrome trabalhando
        target_url           : URL do sistema alvo (padrao: TARGET_URL do .env)
        origem               : quem disparou a execucao (gravado no log de inicio)
        simular_falha_rodada : (teste RF07) rodada em que uma falha sera simulada
        tentativas_com_falha : (teste RF07) quantas tentativas dessa rodada falham
    """
    logger = get_run_logger()
    target_url = target_url or os.getenv("TARGET_URL", "https://rpachallenge.com/")
    logger.info(f"Iniciando orquestracao para '{caminho_planilha}' (alvo: {target_url}).")

    execucao_id = registrar_inicio(caminho_planilha, origem)

    if simular_falha_rodada is not None and tentativas_com_falha > 0:
        # Deixa registrado que esta execucao e um teste de falha, e nao um erro real
        registrar_log(execucao_id, "WARNING", "teste",
                      f"Modo de teste RF07: falha simulada na rodada {simular_falha_rodada} "
                      f"nas {tentativas_com_falha} primeira(s) tentativa(s). "
                      f"Esperas entre tentativas: {ESPERAS_RETRY} s.")

    try:
        validos = validar_e_persistir(execucao_id, caminho_planilha)
        processar_validos(execucao_id, validos, headless, target_url,
                          simular_falha_rodada, tentativas_com_falha)
    except Exception as erro:
        # Falha inesperada (ex.: planilha inexistente ou sem as colunas certas).
        # Registra, encerra a Execucao como 'failed' e repassa o erro para que
        # o Prefect tambem marque o run do flow como falho.
        registrar_log(execucao_id, "ERROR", "fluxo", f"Falha inesperada no fluxo: {erro}")
        finalizar_execucao(execucao_id, status="failed")
        # O relatorio tambem e enviado quando a execucao falha (RF08)
        enviar_relatorio_email(execucao_id)
        raise

    resumo = finalizar_execucao(execucao_id)
    enviar_relatorio_email(execucao_id)
    logger.info(f"Resumo da execucao: {resumo}")
    return resumo


# ---------------------------------------------------------------------------
# Flow de monitoramento do diretorio (RF01)
# ---------------------------------------------------------------------------

def ultima_execucao_do_arquivo(nome_arquivo: str) -> int | None:
    """
    Devolve o id da Execucao mais recente registrada para um arquivo.
    Usado quando o subflow falha: a excecao nao traz o execucao_id, mas a
    Execucao ja foi criada no banco (com status 'failed') e pode ser localizada.
    Devolve None se a falha aconteceu antes de a Execucao ser criada.
    """
    with database.SessionLocal() as sessao:
        return sessao.scalar(
            select(Execucao.id)
            .where(Execucao.arquivo == nome_arquivo)
            .order_by(Execucao.id.desc())
            .limit(1)
        )


@flow(name="Monitoramento do diretorio de entrada")
def fluxo_monitoramento(headless: bool = True, target_url: str | None = None) -> dict | None:
    """
    Flow executado de forma agendada (ver src/servir.py).

    A cada execucao:
      1. procura uma planilha nova no diretorio monitorado (WATCH_DIR);
      2. se nao houver, registra e encerra rapidamente;
      3. se houver, processa a planilha com fluxo_cadastro_colaboradores
         (executado como SUBFLOW: aparece aninhado a este run na Prefect UI);
      4. move a planilha para 'processados' (sucesso) ou 'erro' (falha), com
         data e hora no nome, para que ela nao seja processada de novo.
    """
    logger = get_run_logger()

    arquivo = buscar_arquivo_novo()
    if arquivo is None:
        logger.info("Nenhum arquivo novo no diretório monitorado")
        return None

    logger.info(f"Arquivo novo detectado no diretorio monitorado: {arquivo}")

    try:
        resumo = fluxo_cadastro_colaboradores(
            caminho_planilha=str(arquivo),
            headless=headless,
            target_url=target_url,
            origem="diretorio monitorado",
        )
    except Exception as erro:
        # O subflow falhou (ex.: planilha sem as colunas obrigatorias).
        # Move o arquivo para 'erro' para que ele nao seja reprocessado a cada
        # agendamento, travando a fila de arquivos.
        destino = mover_para_erro(arquivo)
        execucao_id = ultima_execucao_do_arquivo(arquivo.name)
        mensagem = (f"Processamento de '{arquivo.name}' falhou ({erro}). "
                    f"Arquivo movido para '{destino}'.")
        if execucao_id is not None:
            registrar_log(execucao_id, "ERROR", "monitoramento", mensagem)
        else:
            logger.error(f"[monitoramento] {mensagem}")
        # Repassa o erro para que este run tambem apareca como falho na UI.
        raise

    destino = mover_para_processados(arquivo)
    registrar_log(resumo["execucao_id"], "INFO", "monitoramento",
                  f"Arquivo '{arquivo.name}' processado e movido para '{destino}'.")
    return resumo


if __name__ == "__main__":
    # Garante que as tabelas existam (nao recria nem apaga nada se ja existirem)
    init_db()

    # headless=False para voce VER o Chrome preenchendo na primeira execucao
    resultado = fluxo_cadastro_colaboradores(
        caminho_planilha="data/entrada/colaboradores.xlsx",
        headless=False,
    )
    print(resultado)
