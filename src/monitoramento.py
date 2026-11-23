"""
Monitoramento do diretorio de entrada (RF01).

O fluxo agendado (fluxo_monitoramento, em src/fluxo.py) usa estas funcoes para:
  1. procurar uma planilha nova no diretorio monitorado (WATCH_DIR do .env);
  2. depois do processamento, tirar a planilha do diretorio, movendo-a para
     'processados' (deu certo) ou 'erro' (falhou). Assim o mesmo arquivo
     nunca e processado duas vezes e um arquivo com problema nao trava as
     proximas execucoes.

Estrutura de pastas:
    data/entrada/                <- o usuario solta a planilha aqui
    data/entrada/processados/    <- planilhas ja processadas
    data/entrada/erro/           <- planilhas cujo processamento falhou
"""
import os
import shutil
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Raiz do projeto (pasta acima de src/). Caminhos relativos do .env sao
# resolvidos a partir dela, independentemente de onde o Python foi iniciado.
RAIZ_PROJETO = Path(__file__).resolve().parent.parent

EXTENSOES_ACEITAS = {".xlsx"}


def diretorio_monitorado() -> Path:
    """Devolve o caminho absoluto do diretorio monitorado (WATCH_DIR)."""
    diretorio = Path(os.getenv("WATCH_DIR", "./data/entrada"))
    if not diretorio.is_absolute():
        diretorio = RAIZ_PROJETO / diretorio
    diretorio.mkdir(parents=True, exist_ok=True)
    return diretorio.resolve()


def buscar_arquivo_novo() -> Path | None:
    """
    Procura uma planilha nova no diretorio monitorado.

    Considera apenas arquivos .xlsx soltos no diretorio (as subpastas
    'processados' e 'erro' sao ignoradas). Se houver mais de um, devolve o
    mais antigo, para que os arquivos sejam processados na ordem de chegada.
    Devolve None se nao houver nenhum.
    """
    candidatos = [
        arquivo for arquivo in diretorio_monitorado().iterdir()
        if arquivo.is_file()
        and arquivo.suffix.lower() in EXTENSOES_ACEITAS
        # O Excel cria um arquivo temporario "~$nome.xlsx" enquanto a
        # planilha esta aberta; ele nao e uma planilha de verdade.
        and not arquivo.name.startswith("~$")
    ]
    if not candidatos:
        return None
    return min(candidatos, key=lambda arquivo: arquivo.stat().st_mtime)


def _mover_com_carimbo(caminho: str | Path, subpasta: str) -> Path:
    """
    Move o arquivo para uma subpasta do diretorio monitorado, acrescentando
    data e hora ao nome. Exemplo:
        colaboradores.xlsx -> processados/colaboradores_20261117_180012.xlsx
    O carimbo evita sobrescrever um arquivo de mesmo nome enviado antes.
    """
    origem = Path(caminho)
    destino_dir = diretorio_monitorado() / subpasta
    destino_dir.mkdir(parents=True, exist_ok=True)

    carimbo = datetime.now().strftime("%Y%m%d_%H%M%S")
    destino = destino_dir / f"{origem.stem}_{carimbo}{origem.suffix}"
    shutil.move(str(origem), str(destino))
    return destino


def mover_para_processados(caminho: str | Path) -> Path:
    """Move a planilha processada com sucesso para data/entrada/processados."""
    return _mover_com_carimbo(caminho, "processados")


def mover_para_erro(caminho: str | Path) -> Path:
    """Move a planilha cujo processamento falhou para data/entrada/erro."""
    return _mover_com_carimbo(caminho, "erro")
