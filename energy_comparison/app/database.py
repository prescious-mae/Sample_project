from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/your_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def get_io_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
