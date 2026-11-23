"""
Configuracao da conexao com o banco de dados da aplicacao (banco 'rpa').
Usa SQLAlchemy 2.x. A URL de conexao vem da variavel de ambiente DATABASE_URL.
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://prefect:prefect@localhost:5433/rpa",
)

# echo=False para nao poluir o log; mude para True se quiser ver o SQL gerado
engine = create_engine(DATABASE_URL, echo=False, future=True)

SessionLocal = sessionmaker(bind=engine, autoflush=False, future=True)

Base = declarative_base()


def init_db():
    """Cria todas as tabelas no banco, caso ainda nao existam."""
    # Importa pelo nome do modulo para usar o mesmo Base dos models,
    # mesmo quando este arquivo e executado com "python -m src.database".
    from src.database import Base, engine
    import src.models  # noqa: F401  (garante que os models sejam registrados)
    Base.metadata.create_all(bind=engine)
    print("Tabelas criadas/verificadas com sucesso no banco 'rpa'.")


if __name__ == "__main__":
    init_db()
