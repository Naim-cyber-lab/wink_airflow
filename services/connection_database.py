# database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager

POSTGRES_USER = "myuser"
POSTGRES_PASSWORD = "mypassword"
POSTGRES_DB = "mydatabase"
POSTGRES_HOST = "192.168.1.208"
POSTGRES_PORT = "5432"

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

class Database:
    def __init__(self, url: str = DATABASE_URL):
        self.engine = create_engine(url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    @contextmanager
    def get_session(self):
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()

# Instance globale à réutiliser
db_instance = Database()
