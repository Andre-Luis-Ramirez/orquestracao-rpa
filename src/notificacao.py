"""
Relatorio de execucao por e-mail (RF08).

Este modulo tem duas responsabilidades:
  1. montar_relatorio(execucao_id): consulta o banco e monta o conteudo do
     relatorio (assunto, versao em texto simples e versao em HTML);
  2. enviar_email(...): envia a mensagem via SMTP com TLS, usando as
     credenciais do .env (nunca escritas no codigo).

A task enviar_relatorio_email, em src/fluxo.py, junta as duas coisas no
final de cada execucao e registra o resultado do envio na tabela Log.

Variaveis do .env (exemplo para Gmail com senha de app):
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587
    SMTP_USUARIO=seu.email@gmail.com
    SMTP_SENHA=senha de app de 16 letras, sem espacos
    EMAIL_DESTINATARIO=destino@exemplo.com  (varios: separe por virgula)

Comandos para teste manual (na raiz do projeto):
    python -m src.notificacao teste            envia um e-mail simples (valida as credenciais)
    python -m src.notificacao previa 7         mostra o relatorio da execucao 7, sem enviar
    python -m src.notificacao execucao 7       envia o relatorio da execucao 7 (grava na tabela Log)
"""
import html
import os
import smtplib
import ssl
import sys
from email.message import EmailMessage
from email.utils import formatdate

from dotenv import load_dotenv
from sqlalchemy import select

load_dotenv()

from src import database  # noqa: E402  (acesso pelo modulo: ver comentario em src/fluxo.py)
from src.models import Execucao, Colaborador  # noqa: E402

VARIAVEIS_SMTP = ["SMTP_HOST", "SMTP_PORT", "SMTP_USUARIO", "SMTP_SENHA", "EMAIL_DESTINATARIO"]

# Mensagens de erro do Selenium podem ter centenas de linhas; no e-mail basta o inicio
TAMANHO_MAXIMO_ERRO = 300

STATUS_LEGIVEL = {"completed": "concluída", "failed": "falhou", "running": "em andamento"}


class ConfiguracaoEmailAusente(RuntimeError):
    """Lancada quando faltam variaveis de e-mail no .env."""


# ---------------------------------------------------------------------------
# Envio
# ---------------------------------------------------------------------------

def ler_configuracao() -> dict:
    """Le e confere as variaveis de SMTP do .env."""
    faltando = [nome for nome in VARIAVEIS_SMTP if not os.getenv(nome, "").strip()]
    if faltando:
        raise ConfiguracaoEmailAusente(
            "variaveis de e-mail ausentes no .env: " + ", ".join(faltando))

    return {
        "host": os.getenv("SMTP_HOST").strip(),
        "porta": int(os.getenv("SMTP_PORT")),
        "usuario": os.getenv("SMTP_USUARIO").strip(),
        "senha": os.getenv("SMTP_SENHA"),
        "destinatarios": [email.strip() for email in os.getenv("EMAIL_DESTINATARIO").split(",")
                          if email.strip()],
    }


def enviar_email(assunto: str, corpo_texto: str, corpo_html: str | None = None) -> list[str]:
    """
    Envia um e-mail via SMTP com TLS e devolve a lista de destinatarios.

    - Porta 587 (Gmail e a maioria dos provedores): a conexao comeca sem
      criptografia e e protegida com STARTTLS antes do login.
    - Porta 465: a conexao ja comeca criptografada (SMTP sobre SSL/TLS).

    Em caso de problema (credencial errada, sem internet, servidor fora do ar)
    a excecao e propagada; quem chama decide o que fazer com ela.
    """
    config = ler_configuracao()

    mensagem = EmailMessage()
    mensagem["Subject"] = assunto
    mensagem["From"] = config["usuario"]
    mensagem["To"] = ", ".join(config["destinatarios"])
    mensagem["Date"] = formatdate(localtime=True)
    mensagem.set_content(corpo_texto)                      # versao em texto simples
    if corpo_html:
        mensagem.add_alternative(corpo_html, subtype="html")  # versao formatada

    contexto_tls = ssl.create_default_context()  # valida o certificado do servidor

    if config["porta"] == 465:
        with smtplib.SMTP_SSL(config["host"], config["porta"],
                              context=contexto_tls, timeout=30) as servidor:
            servidor.login(config["usuario"], config["senha"])
            servidor.send_message(mensagem)
    else:
        with smtplib.SMTP(config["host"], config["porta"], timeout=30) as servidor:
            servidor.ehlo()
            servidor.starttls(context=contexto_tls)
            servidor.ehlo()
            servidor.login(config["usuario"], config["senha"])
            servidor.send_message(mensagem)

    return config["destinatarios"]


# ---------------------------------------------------------------------------
# Conteudo do relatorio
# ---------------------------------------------------------------------------

def _data_hora(valor) -> str:
    return valor.strftime("%d/%m/%Y %H:%M:%S") if valor else "-"


def _duracao(inicio, fim) -> str:
    if not (inicio and fim):
        return "-"
    segundos = int((fim - inicio).total_seconds())
    minutos, segundos = divmod(segundos, 60)
    return f"{minutos} min {segundos} s" if minutos else f"{segundos} s"


def _nome(colaborador: Colaborador) -> str:
    return f"{colaborador.first_name or ''} {colaborador.last_name or ''}".strip() or "(sem nome)"


def _resumir_erro(texto: str | None) -> str:
    texto = " ".join((texto or "").split())  # remove quebras de linha
    if len(texto) > TAMANHO_MAXIMO_ERRO:
        texto = texto[:TAMANHO_MAXIMO_ERRO] + "..."
    return texto or "(sem detalhe)"


def montar_relatorio(execucao_id: int) -> tuple[str, str, str]:
    """
    Consulta a execucao no banco e devolve (assunto, corpo_texto, corpo_html).
    O conteudo atende ao RF08: identificacao, status, horarios, totais,
    registros reprovados na validacao e falhas de cadastro.
    """
    with database.SessionLocal() as sessao:
        execucao = sessao.get(Execucao, execucao_id)
        if execucao is None:
            raise ValueError(f"Execucao {execucao_id} nao encontrada no banco.")

        reprovados = sessao.scalars(
            select(Colaborador)
            .where(Colaborador.execucao_id == execucao_id, Colaborador.valido.is_(False))
            .order_by(Colaborador.id)
        ).all()
        falhas = sessao.scalars(
            select(Colaborador)
            .where(Colaborador.execucao_id == execucao_id,
                   Colaborador.status_cadastro == "falha")
            .order_by(Colaborador.id)
        ).all()

        # Copia os dados para listas simples antes de fechar a sessao
        status = f"{execucao.status} ({STATUS_LEGIVEL.get(execucao.status, execucao.status)})"
        cabecalho = [
            ("Execução", str(execucao.id)),
            ("Arquivo", execucao.arquivo),
            ("Status", status),
            ("Início", _data_hora(execucao.iniciada_em)),
            ("Fim", _data_hora(execucao.finalizada_em)),
            ("Duração", _duracao(execucao.iniciada_em, execucao.finalizada_em)),
        ]
        totais = [
            ("Registros na planilha", execucao.total_registros),
            ("Válidos", execucao.total_validos),
            ("Cadastrados com sucesso", execucao.total_sucesso),
            ("Falhas de cadastro", execucao.total_falha),
        ]
        lista_reprovados = [(_nome(c), c.motivo_invalido or "-") for c in reprovados]
        lista_falhas = [(_nome(c), _resumir_erro(c.detalhe_erro)) for c in falhas]
        assunto = (f"[RPA] Execução {execucao.id} - {STATUS_LEGIVEL.get(execucao.status, execucao.status)}"
                   f" - {execucao.total_sucesso}/{execucao.total_validos} cadastrados")

    # --- Versao em texto simples ---
    linhas = [f"Relatório da execução {execucao_id}", ""]
    linhas += [f"{rotulo}: {valor}" for rotulo, valor in cabecalho]
    linhas += ["", "Totais"]
    linhas += [f"  {rotulo}: {valor}" for rotulo, valor in totais]
    linhas += ["", f"Registros reprovados na validação ({len(lista_reprovados)})"]
    linhas += [f"  - {nome}: {motivo}" for nome, motivo in lista_reprovados] or ["  Nenhum."]
    linhas += ["", f"Falhas de cadastro ({len(lista_falhas)})"]
    linhas += [f"  - {nome}: {erro}" for nome, erro in lista_falhas] or ["  Nenhuma."]
    linhas += ["", "Mensagem automática da Aplicação de Orquestração RPA (Prefect)."]
    corpo_texto = "\n".join(linhas)

    # --- Versao em HTML (os valores vindos da planilha sao escapados) ---
    e = html.escape

    def tabela(linhas_tabela, colunas=None):
        estilo_celula = "border:1px solid #ccc;padding:4px 8px;text-align:left"
        partes = ['<table style="border-collapse:collapse;margin-bottom:16px">']
        if colunas:
            partes.append("<tr>" + "".join(
                f'<th style="{estilo_celula};background:#f0f0f0">{e(c)}</th>' for c in colunas) + "</tr>")
        for linha in linhas_tabela:
            partes.append("<tr>" + "".join(
                f'<td style="{estilo_celula}">{e(str(v))}</td>' for v in linha) + "</tr>")
        partes.append("</table>")
        return "".join(partes)

    def secao(titulo, itens, colunas, vazio):
        corpo = tabela(itens, colunas) if itens else f"<p>{vazio}</p>"
        return f"<h3>{e(titulo)} ({len(itens)})</h3>{corpo}"

    corpo_html = (
        '<div style="font-family:Arial,sans-serif;font-size:14px">'
        f"<h2>Relatório da execução {execucao_id}</h2>"
        + tabela(cabecalho)
        + "<h3>Totais</h3>" + tabela(totais)
        + secao("Registros reprovados na validação", lista_reprovados,
                ["Colaborador", "Motivo"], "Nenhum.")
        + secao("Falhas de cadastro", lista_falhas, ["Colaborador", "Erro"], "Nenhuma.")
        + '<p style="color:#777">Mensagem automática da Aplicação de Orquestração RPA (Prefect).</p>'
        "</div>"
    )

    return assunto, corpo_texto, corpo_html


# ---------------------------------------------------------------------------
# Testes manuais
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    comando = sys.argv[1] if len(sys.argv) > 1 else ""

    if comando == "teste":
        destinos = enviar_email("[RPA] Teste de configuração de e-mail",
                                "Se você recebeu esta mensagem, o SMTP está configurado corretamente.")
        print(f"E-mail de teste enviado para: {', '.join(destinos)}")

    elif comando == "previa" and len(sys.argv) > 2:
        assunto, texto, _ = montar_relatorio(int(sys.argv[2]))
        print(f"Assunto: {assunto}\n\n{texto}")

    elif comando == "execucao" and len(sys.argv) > 2:
        # Usa a mesma task do fluxo: o resultado do envio vai para a tabela Log
        from src.fluxo import enviar_relatorio_email
        enviado = enviar_relatorio_email(int(sys.argv[2]))
        print("Relatório enviado." if enviado else "Relatório NÃO enviado (veja a tabela Log).")

    else:
        print(__doc__)
