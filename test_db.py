from db import engine
from sqlalchemy import text

try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
        print("✅ Koneksi ke database berhasil:", result)
except Exception as e:
    print("❌ Gagal konek ke database:", e)