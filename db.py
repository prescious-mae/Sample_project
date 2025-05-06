from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus

# Database connection details
DB_USER = "datateam"
DB_PASSWORD = quote_plus("v3ry$tr0ngP@ssw0rd!") 
DB_HOST = "a3rvoa7dz8.dowooytki2.tsdb.cloud.timescale.com"
DB_PORT = "34482"
DB_NAME = "tsdb"

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine and session
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db_engine():
    return engine

def get_db_session():
    return SessionLocal()