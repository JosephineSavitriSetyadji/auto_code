import pandas as pd
from sqlalchemy import text
from db import engine

# === 1. Baca CSV mentah ===
df = pd.read_csv("data_mentah.csv")
print("🧾 Data mentah berhasil dibaca!")

# === 2. Ambil data referensi dari database ===
with engine.connect() as conn:
    prov_df = pd.read_sql(text("SELECT province_code AS kode_provinsi, province AS provinsi FROM provinces"), conn)
    kab_df = pd.read_sql(text("SELECT province_code AS kode_provinsi, city_code AS kode_kabkot, city AS kabkot FROM cities"), conn)
    kec_df = pd.read_sql(text("SELECT city_code AS kode_kabkot, district_code AS kode_kecamatan, district AS kecamatan FROM districts"), conn)
    deskel_df = pd.read_sql(text("SELECT district_code AS kode_kecamatan, village_code AS kode_deskel, village AS deskel FROM villages"), conn)

print("✅ Data referensi berhasil diambil dari database")

# === 3. Normalisasi kolom ===
df.columns = df.columns.str.strip().str.lower()

# Normalisasi nama kolom lokasi
if 'kabupaten/kota' in df.columns:
    df = df.rename(columns={'kabupaten/kota': 'kabkot'})
elif 'kabupaten' in df.columns:
    df = df.rename(columns={'kabupaten': 'kabkot'})

if 'kelurahan' in df.columns:
    df = df.rename(columns={'kelurahan': 'deskel'})

# === 4. Gabung data referensi ===
df = df.merge(prov_df, on="provinsi", how="left")
df = df.merge(kab_df, on=["kabkot", "kode_provinsi"], how="left", suffixes=("", "_kab"))
df = df.merge(kec_df, on=["kecamatan", "kode_kabkot"], how="left", suffixes=("", "_kec"))
df = df.merge(deskel_df, on=["deskel", "kode_kecamatan"], how="left", suffixes=("", "_deskel"))

# === 5. Bersihkan data ===

# RT/RW ubah ke integer tanpa .0
for col in ['rt', 'rw']:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else x)

# Hapus "00:00:00" dan kata "tahun"
for col in df.columns:
    if df[col].astype(str).str.contains("00:00:00").any():
        df[col] = df[col].astype(str).str.replace(" 00:00:00", "", regex=False)
    df[col] = df[col].astype(str).str.replace("tahun", "", case=False, regex=False).str.strip()

# Ubah nilai jenis kelamin
df = df.replace({
    "Laki-laki": "L",
    "Perempuan": "P",
    "LAKI-LAKI": "L",
    "PEREMPUAN": "P"
})

# Pastikan kode numeric gak ada .0
for col in ['kode_provinsi', 'kode_kabkot', 'kode_kecamatan', 'kode_deskel']:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else "")

# === 6. Urutkan posisi kolom (kode di sebelah nama wilayah) ===
order_cols = []
added_cols = set()

for col in df.columns:
    if col == "provinsi":
        order_cols.extend(["provinsi", "kode_provinsi"])
        added_cols.update(["provinsi", "kode_provinsi"])
    elif col == "kabkot":
        order_cols.extend(["kabkot", "kode_kabkot"])
        added_cols.update(["kabkot", "kode_kabkot"])
    elif col == "kecamatan":
        order_cols.extend(["kecamatan", "kode_kecamatan"])
        added_cols.update(["kecamatan", "kode_kecamatan"])
    elif col == "deskel":
        order_cols.extend(["deskel", "kode_deskel"])
        added_cols.update(["deskel", "kode_deskel"])
    elif col not in added_cols:
        order_cols.append(col)
        added_cols.add(col)

df = df[[col for col in order_cols if col in df.columns]]

df = df.fillna("")

# === 7. Simpan hasil ===
df.to_csv("data_hasil.csv", index=False)
print("🎉 Data lengkap (provinsi → kabkot → kecamatan → deskel) berhasil disimpan ke data_hasil.csv")
