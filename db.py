from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Baca file .env
load_dotenv()

# Ambil variabel koneksi
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Buat koneksi ke MySQL
DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Buat engine SQLAlchemy
engine = create_engine(DB_URL, echo=False)