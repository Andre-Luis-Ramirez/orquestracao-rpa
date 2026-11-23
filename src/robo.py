"""
Robo de automacao (RPA) para o RPA Challenge (https://rpachallenge.com/).

Responsavel pelo RF04: navegar pela interface do sistema alvo, preencher
os formularios e submeter o cadastro de cada colaborador.

Particularidade do RPA Challenge:
  Os campos do formulario mudam de posicao a cada submit. Por isso, cada
  campo e localizado pelo atributo 'ng-reflect-name', que permanece estavel
  independentemente da posicao visual. Essa estrategia, combinada com esperas
  explicitas, garante a robustez do robo diante das variacoes da interface.

O site processa uma rodada de exatamente 10 registros por vez: clica-se em
'Start', preenchem-se os 10 formularios em sequencia e, ao final, o site
exibe a taxa de sucesso.
"""
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Mapeamento campo da planilha -> atributo ng-reflect-name no formulario
CAMPOS = {
    "first_name": "labelFirstName",
    "last_name": "labelLastName",
    "company_name": "labelCompanyName",
    "role_in_company": "labelRole",
    "address": "labelAddress",
    "email": "labelEmail",
    "phone_number": "labelPhone",
}

TAMANHO_RODADA = 10  # o RPA Challenge processa 10 registros por rodada


def criar_driver(headless: bool = True) -> webdriver.Chrome:
    """Cria e configura o driver do Chrome."""
    opcoes = Options()
    if headless:
        opcoes.add_argument("--headless=new")
    opcoes.add_argument("--disable-gpu")
    opcoes.add_argument("--window-size=1280,900")
    opcoes.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=opcoes)
    return driver


def preencher_formulario(driver, espera, registro: dict) -> None:
    """
    Preenche os sete campos de um formulario com os dados de um registro.
    Cada campo e localizado pelo atributo ng-reflect-name (posicao-independente).
    """
    for campo_planilha, atributo in CAMPOS.items():
        seletor = (By.CSS_SELECTOR, f"input[ng-reflect-name='{atributo}']")
        # espera explicita: aguarda o campo estar presente antes de preencher
        elemento = espera.until(EC.presence_of_element_located(seletor))
        elemento.clear()
        elemento.send_keys(str(registro.get(campo_planilha, "")))


def processar_rodada(registros: list[dict], headless: bool = True,
                     target_url: str = "https://rpachallenge.com/") -> dict:
    """
    Processa uma rodada de ate 10 registros no RPA Challenge.

    Recebe a lista de registros validos (ja filtrados) e devolve um dicionario
    com o resultado de cada um e a mensagem final exibida pelo site.

    Retorno:
        {
          "resultados": [ {registro..., "status_cadastro": "sucesso"|"falha",
                           "detalhe_erro": str}, ... ],
          "mensagem_site": str
        }
    """
    if len(registros) > TAMANHO_RODADA:
        raise ValueError(
            f"Uma rodada processa no maximo {TAMANHO_RODADA} registros; "
            f"recebidos {len(registros)}."
        )

    driver = criar_driver(headless=headless)
    espera = WebDriverWait(driver, 15)
    resultados = []

    try:
        driver.get(target_url)

        # Clica em Start para iniciar a rodada.
        # Localiza pelo texto do botao, que e mais estavel que as classes de estilo.
        botao_start = espera.until(
            EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='Start']"))
        )
        botao_start.click()

        # Preenche e submete cada registro
        for registro in registros:
            try:
                preencher_formulario(driver, espera, registro)
                botao_submit = espera.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='Submit']"))
                )
                botao_submit.click()
                registro_resultado = dict(registro)
                registro_resultado["status_cadastro"] = "sucesso"
                registro_resultado["detalhe_erro"] = None
            except Exception as e:
                registro_resultado = dict(registro)
                registro_resultado["status_cadastro"] = "falha"
                registro_resultado["detalhe_erro"] = str(e)
            resultados.append(registro_resultado)

        # Le a mensagem final de resultado exibida pelo site
        try:
            mensagem = espera.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.message2"))
            ).text
        except Exception:
            mensagem = "(mensagem de resultado nao localizada)"

    finally:
        driver.quit()

    return {"resultados": resultados, "mensagem_site": mensagem}


if __name__ == "__main__":
    # Teste isolado: processa uma rodada com os 10 primeiros registros validos
    from src.validacao import validar_planilha

    registros = validar_planilha("data/entrada/colaboradores.xlsx")
    validos = [r for r in registros if r["valido"]][:TAMANHO_RODADA]

    print(f"Processando {len(validos)} registros no RPA Challenge...")
    # headless=False para voce VER o Chrome preenchendo (bom para o primeiro teste)
    saida = processar_rodada(validos, headless=False)

    sucessos = sum(1 for r in saida["resultados"] if r["status_cadastro"] == "sucesso")
    falhas = sum(1 for r in saida["resultados"] if r["status_cadastro"] == "falha")

    print(f"Sucessos : {sucessos}")
    print(f"Falhas   : {falhas}")
    print(f"Mensagem do site: {saida['mensagem_site']}")
