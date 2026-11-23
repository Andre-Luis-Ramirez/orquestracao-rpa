"""
Leitura e validacao da planilha de colaboradores.

Responsavel pelo RF02: ler o arquivo detectado, extrair os registros
e validar os dados antes do processamento pelo robo RPA.

Regras de validacao:
  1. Todos os sete campos devem estar preenchidos (nao vazios).
  2. O e-mail deve ter formato valido (algo@algo.dominio).
  3. O telefone deve conter apenas digitos, com ao menos 8 numeros.

Cada registro invalido recebe o motivo da reprovacao, que sera
registrado no banco para fins de rastreabilidade.
"""
import re
import pandas as pd

# Colunas esperadas na planilha (casam com o formulario do RPA Challenge)
COLUNAS_ESPERADAS = [
    "first_name",
    "last_name",
    "company_name",
    "role_in_company",
    "address",
    "email",
    "phone_number",
]

# Expressao regular simples para validar e-mail
REGEX_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def ler_planilha(caminho: str) -> pd.DataFrame:
    """
    Le a planilha .xlsx e devolve um DataFrame com as colunas esperadas.
    Lanca ValueError se alguma coluna obrigatoria estiver ausente.
    """
    df = pd.read_excel(caminho, dtype=str)  # dtype=str evita que numeros virem float
    df = df.fillna("")                      # celulas vazias viram string vazia

    faltantes = [c for c in COLUNAS_ESPERADAS if c not in df.columns]
    if faltantes:
        raise ValueError(
            f"A planilha nao possui as colunas obrigatorias: {', '.join(faltantes)}"
        )

    # Mantem apenas as colunas esperadas, na ordem correta
    df = df[COLUNAS_ESPERADAS]
    # Remove espacos em branco nas pontas de cada campo
    for coluna in COLUNAS_ESPERADAS:
        df[coluna] = df[coluna].str.strip()

    return df


def validar_registro(registro: dict) -> tuple[bool, str]:
    """
    Valida um unico registro (dict com as sete chaves).
    Retorna (True, "") se valido, ou (False, motivo) se invalido.
    """
    # Regra 1: campos obrigatorios preenchidos
    for campo in COLUNAS_ESPERADAS:
        if not registro.get(campo):
            return False, f"Campo obrigatorio vazio: {campo}"

    # Regra 2: e-mail com formato valido
    if not REGEX_EMAIL.match(registro["email"]):
        return False, f"E-mail invalido: {registro['email']}"

    # Regra 3: telefone apenas com digitos e ao menos 8 numeros
    telefone = registro["phone_number"]
    if not telefone.isdigit() or len(telefone) < 8:
        return False, f"Telefone invalido: {telefone}"

    return True, ""


def validar_planilha(caminho: str) -> list[dict]:
    """
    Le e valida a planilha inteira.
    Retorna uma lista de dicionarios, um por registro, cada um contendo
    os sete campos mais as chaves 'valido' (bool) e 'motivo_invalido' (str).
    """
    df = ler_planilha(caminho)
    resultado = []

    for _, linha in df.iterrows():
        registro = linha.to_dict()
        valido, motivo = validar_registro(registro)
        registro["valido"] = valido
        registro["motivo_invalido"] = motivo
        resultado.append(registro)

    return resultado


if __name__ == "__main__":
    # Teste rapido: valida a planilha de exemplo e imprime um resumo
    import sys

    caminho = sys.argv[1] if len(sys.argv) > 1 else "data/entrada/colaboradores.xlsx"
    registros = validar_planilha(caminho)

    validos = [r for r in registros if r["valido"]]
    invalidos = [r for r in registros if not r["valido"]]

    print(f"Total de registros : {len(registros)}")
    print(f"Validos            : {len(validos)}")
    print(f"Invalidos          : {len(invalidos)}")
    print()
    if invalidos:
        print("Registros reprovados na validacao:")
        for r in invalidos:
            nome = f"{r['first_name']} {r['last_name']}".strip() or "(sem nome)"
            print(f"  - {nome}: {r['motivo_invalido']}")
