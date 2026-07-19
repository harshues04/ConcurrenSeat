from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import get_settings

settings = get_settings()

# Sized for flash-sale bursts: many threads briefly need a connection at the
# same instant. 20+60 stays safely under Postgres's default max_connections=100.
engine = create_engine(
    settings.database_url, pool_pre_ping=True, pool_size=20, max_overflow=60
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
