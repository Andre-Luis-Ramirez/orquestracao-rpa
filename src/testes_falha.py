"""
Cenarios reprodutiveis de falha e reexecucao automatica (RF07).

Uso (na raiz do projeto, com os containers do docker-compose no ar):

    python -m src.testes_falha recuperada
        A rodada 2 falha nas 2 primeiras tentativas e o retry a recupera na 3a.

    python -m src.testes_falha definitiva
        A rodada 2 falha em todas as 4 tentativas. O fluxo marca os
        colaboradores dessa rodada como 'falha', registra ERROR na tabela Log
        e segue normalmente com as rodadas 3, 4 e 5.

    python -m src.testes_falha url-invalida
        Falha real: o robo acessa um endereco inexistente, entao TODAS as
        rodadas esgotam as tentativas. E o cenario mais lento (cada tentativa
        espera ~15 s pelo botao Start antes de desistir).

Opcoes:
    --planilha CAMINHO   planilha usada (padrao: data/entrada/colaboradores.xlsx)
    --rodada N           rodada em que a falha e simulada (padrao: 2)
    --esperas "3,3,3"    esperas entre tentativas, em segundos (padrao: 3,3,3)
    --ver-chrome         mostra o Chrome trabalhando (headless=False)

Os cenarios usam esperas curtas para o teste nao demorar. O padrao do
sistema (10, 30 e 60 s) continua valendo nas execucoes normais.

IMPORTANTE: pare o "python -m src.servir" antes de rodar estes testes. Se ele
estiver ativo, o monitoramento pode pegar a planilha de data/entrada no meio
do teste e move-la para 'processados'.
"""
import argparse
import os
import sys
from pathlib import Path

CENARIOS = {
    "recuperada": "falha nas 2 primeiras tentativas da rodada; o retry recupera na 3a",
    "definitiva": "falha em todas as tentativas da rodada; o fluxo segue para a proxima",
    "url-invalida": "endereco do sistema alvo inexistente; todas as rodadas falham",
}

# Dominio reservado (RFC 2606): ".invalid" nunca existe na internet
URL_INVALIDA = "https://rpachallenge.invalid/"


def main() -> None:
    parser = argparse.ArgumentParser(description="Cenarios de teste do RF07 (retry).")
    parser.add_argument("cenario", choices=CENARIOS.keys())
    parser.add_argument("--planilha", default="data/entrada/colaboradores.xlsx")
    parser.add_argument("--rodada", type=int, default=2)
    parser.add_argument("--esperas", default="3,3,3")
    parser.add_argument("--ver-chrome", action="store_true")
    args = parser.parse_args()

    # As esperas do retry sao lidas quando src.fluxo e importado, por isso a
    # variavel precisa ser definida ANTES do import abaixo.
    os.environ["RETRY_DELAYS"] = args.esperas

    from src.database import init_db
    from src.fluxo import fluxo_cadastro_colaboradores, RETRIES_RODADA

    if not Path(args.planilha).is_file():
        sys.exit(f"Planilha nao encontrada: {args.planilha}\n"
                 f"Copie uma planilha para esse caminho ou use --planilha.")

    parametros = {
        "caminho_planilha": args.planilha,
        "headless": not args.ver_chrome,
        "origem": f"teste RF07 ({args.cenario})",
    }
    if args.cenario == "recuperada":
        parametros["simular_falha_rodada"] = args.rodada
        parametros["tentativas_com_falha"] = 2
    elif args.cenario == "definitiva":
        parametros["simular_falha_rodada"] = args.rodada
        parametros["tentativas_com_falha"] = RETRIES_RODADA + 1  # todas falham
    else:
        parametros["target_url"] = URL_INVALIDA

    print(f"Cenario '{args.cenario}': {CENARIOS[args.cenario]}.")
    print(f"Esperas entre tentativas: {args.esperas} s.\n")

    init_db()
    resumo = fluxo_cadastro_colaboradores(**parametros)

    print("\nResumo:", resumo)
    print(f"Consulte os logs desta execucao no DBeaver com execucao_id = {resumo['execucao_id']} "
          f"(consultas em sql/consultas_rf07.sql).")


if __name__ == "__main__":
    main()
