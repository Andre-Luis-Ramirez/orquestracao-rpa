"""
Publica o fluxo de monitoramento como um DEPLOYMENT agendado no Prefect 3 (RF01).

Uso (na raiz do projeto, com os containers do docker-compose no ar):
    python -m src.servir

O comando fica em execucao ("servindo" o flow): ele registra o deployment
"monitoramento-diretorio" no Prefect Server e executa cada run agendado.
Para parar, use Ctrl+C. Enquanto ele estiver parado, nada e processado.

Agendamento (lido do .env):
  - AGENDAMENTO_CRON: expressao cron, ex. "0 18 * * *" (todo dia as 18h).
    Se estiver definida, tem prioridade sobre o intervalo.
  - AGENDAMENTO_FUSO: fuso horario do cron (padrao "America/Sao_Paulo").
    Sem fuso, o Prefect interpreta o cron em UTC: "0 18 * * *" rodaria as
    15h no horario de Brasilia.
  - AGENDAMENTO_INTERVALO_SEGUNDOS: intervalo entre execucoes (padrao 60,
    bom para testes). Usado quando AGENDAMENTO_CRON nao esta definida.
  - MONITORAMENTO_HEADLESS: "false" para ver o Chrome trabalhando (padrao "true").
"""
import os
from datetime import timedelta

from prefect.schedules import Cron, Interval

# Importar o flow tambem carrega o .env e prepara o caminho do projeto.
from src.fluxo import fluxo_monitoramento

NOME_DEPLOYMENT = "monitoramento-diretorio"


def montar_agendamento():
    """
    Monta o agendamento a partir do .env.
    Devolve o objeto de agendamento do Prefect e uma descricao legivel.
    """
    cron = os.getenv("AGENDAMENTO_CRON", "").strip()
    if cron:
        fuso = os.getenv("AGENDAMENTO_FUSO", "America/Sao_Paulo").strip()
        return Cron(cron, timezone=fuso), f"cron '{cron}' no fuso {fuso}"

    segundos = int(os.getenv("AGENDAMENTO_INTERVALO_SEGUNDOS", "60"))
    return Interval(timedelta(seconds=segundos)), f"a cada {segundos} segundo(s)"


def ler_booleano(nome: str, padrao: bool) -> bool:
    """Le uma variavel do .env como verdadeiro/falso ("true", "1", "sim"...)."""
    valor = os.getenv(nome)
    if valor is None or not valor.strip():
        return padrao
    return valor.strip().lower() in {"true", "1", "sim", "s", "yes", "y"}


if __name__ == "__main__":
    agendamento, descricao = montar_agendamento()
    headless = ler_booleano("MONITORAMENTO_HEADLESS", True)

    print(f"Publicando o deployment '{NOME_DEPLOYMENT}' ({descricao}, headless={headless}).")

    fluxo_monitoramento.serve(
        name=NOME_DEPLOYMENT,
        schedules=[agendamento],
        parameters={"headless": headless},
        description="RF01: verifica o diretorio monitorado e processa planilhas novas.",
        tags=["tcc", "rf01", "monitoramento"],
        # Apenas um run por vez: um processamento com varias rodadas pode
        # durar mais que o intervalo do agendamento. Sem este limite, o run
        # seguinte pegaria a mesma planilha antes de ela ser movida.
        limit=1,
    )
