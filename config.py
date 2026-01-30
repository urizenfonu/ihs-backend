from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    DATABASE_PATH = os.getenv("DATABASE_PATH") or str((Path(__file__).parent / "data" / "ihs.db").resolve())
    LOG_LEVEL = os.getenv("LOG_LEVEL", "info")
    CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:3001").split(",")
    PORT = int(os.getenv("PORT", "3001"))
    HOST = os.getenv("HOST", "0.0.0.0")

config = Config()
