"""
Modelos de dados (tabelas) da aplicacao, definidos com SQLAlchemy.

Tres tabelas:
  - execucao     : uma linha por execucao do workflow
  - colaborador  : registros extraidos da planilha, vinculados a uma execucao
  - log          : eventos detalhados de cada etapa, vinculados a uma execucao
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text, ForeignKey
)
from sqlalchemy.orm import relationship
from src.database import Base


class Execucao(Base):
    __tablename__ = "execucao"

    id = Column(Integer, primary_key=True, autoincrement=True)
    arquivo = Column(String(255), nullable=False)          # nome do arquivo processado
    status = Column(String(20), nullable=False, default="running")  # running | completed | failed
    iniciada_em = Column(DateTime, nullable=False, default=datetime.now)
    finalizada_em = Column(DateTime, nullable=True)
    total_registros = Column(Integer, default=0)           # total lido da planilha
    total_validos = Column(Integer, default=0)             # passaram na validacao
    total_sucesso = Column(Integer, default=0)             # cadastrados com sucesso
    total_falha = Column(Integer, default=0)               # falharam no cadastro

    colaboradores = relationship(
        "Colaborador", back_populates="execucao", cascade="all, delete-orphan"
    )
    logs = relationship(
        "Log", back_populates="execucao", cascade="all, delete-orphan"
    )


class Colaborador(Base):
    __tablename__ = "colaborador"

    id = Column(Integer, primary_key=True, autoincrement=True)
    execucao_id = Column(Integer, ForeignKey("execucao.id"), nullable=False)

    # Campos extraidos da planilha (casam com o formulario do RPA Challenge)
    first_name = Column(String(100))
    last_name = Column(String(100))
    company_name = Column(String(150))
    role_in_company = Column(String(150))
    address = Column(String(255))
    email = Column(String(150))
    phone_number = Column(String(50))

    # Resultado do processamento
    valido = Column(Boolean, default=True)                 # passou na validacao?
    motivo_invalido = Column(String(255), nullable=True)   # se invalido, por que
    status_cadastro = Column(String(20), nullable=True)    # sucesso | falha | nao_processado
    detalhe_erro = Column(Text, nullable=True)             # mensagem de erro, se houver

    execucao = relationship("Execucao", back_populates="colaboradores")


class Log(Base):
    __tablename__ = "log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    execucao_id = Column(Integer, ForeignKey("execucao.id"), nullable=False)
    momento = Column(DateTime, nullable=False, default=datetime.now)
    nivel = Column(String(10), nullable=False, default="INFO")  # INFO | WARNING | ERROR
    etapa = Column(String(50), nullable=True)              # validacao | rpa | persistencia ...
    mensagem = Column(Text, nullable=False)

    execucao = relationship("Execucao", back_populates="logs")
