from flask import Flask, render_template, request, send_file
import os
import pandas as pd
from sqlalchemy import text
from db import engine

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/')
def index():
    return render_template('index.html', message=None, download_ready=False, download_link=None)

@app.route('/process', methods=['POST'])
def process_file():
    if 'file' not in request.files:
        return render_template('index.html', message='⚠️ Tidak ada file diunggah.', download_ready=False)

    file = request.files['file']
    if file.filename == '':
        return render_template('index.html', message='⚠️ Pilih file CSV dulu.', download_ready=False)

    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(filepath)

    try:
        # === 1. Baca CSV ===
        df = pd.read_csv(filepath)
        df.columns = df.columns.str.strip().str.lower()

        # === 2. Ambil referensi dari database ===
        with engine.connect() as conn:
            prov_df = pd.read_sql(text("SELECT province_code AS kode_provinsi, province AS provinsi FROM provinces"), conn)
            kab_df = pd.read_sql(text("""
                SELECT province_code AS kode_provinsi, city_code AS kode_kabkot, 
                       city AS kabkot 
                FROM cities
            """), conn)
            kec_df = pd.read_sql(text("""
                SELECT city_code AS kode_kabkot, district_code AS kode_kecamatan, 
                       district AS kecamatan 
                FROM districts
            """), conn)
            deskel_df = pd.read_sql(text("""
                SELECT district_code AS kode_kecamatan, village_code AS kode_deskel, 
                       village AS deskel 
                FROM villages
            """), conn)

            agama_df = pd.read_sql(text("SELECT kode_agama, nama_agama AS agama FROM agama"), conn)
            pekerjaan_df = pd.read_sql(text("SELECT kode_pekerjaan, nama_pekerjaan AS pekerjaan FROM pekerjaan"), conn)
            partai_df = pd.read_sql(text("SELECT kode_partai, nama_partai AS partai FROM partai"), conn)

        # === 3. Normalisasi kolom ===
        if "kabupaten/kota" in df.columns:
            df.rename(columns={"kabupaten/kota": "kabkot"}, inplace=True)
        elif "kabupaten" in df.columns:
            df.rename(columns={"kabupaten": "kabkot"}, inplace=True)

        if "kelurahan" in df.columns:
            df.rename(columns={"kelurahan": "deskel"}, inplace=True)

        # UPPER untuk sync ke DB
        for kol in ["provinsi", "kabkot", "kecamatan", "deskel"]:
            if kol in df.columns:
                df[kol] = df[kol].astype(str).str.upper().str.strip()

        # === 4. Join wilayah ===
        df = df.merge(prov_df, on="provinsi", how="left")
        df = df.merge(kab_df, on=["kabkot", "kode_provinsi"], how="left")
        df = df.merge(kec_df, on=["kecamatan", "kode_kabkot"], how="left")
        df = df.merge(deskel_df, on=["deskel", "kode_kecamatan"], how="left")

        df = df.fillna("")

        # === Bersihkan column minor ===
        if "usia" in df.columns:
            df["usia"] = (
                df["usia"].astype(str)
                .str.replace("tahun", "", case=False)
                .str.strip()
            )

        df = df.replace({
            "Laki-laki": "L", "LAKI-LAKI": "L",
            "Perempuan": "P", "PEREMPUAN": "P"
        })

        for col in df.columns:
            df[col] = df[col].astype(str).str.replace(" 00:00:00", "", regex=False)

        # pastikan kode numeric rapih
        for col in ['rt_', 'rw', 'kode_provinsi', 'kode_kabkot', 'kode_kecamatan', 'kode_deskel']:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: str(int(float(x))) if x.replace('.', '', 1).isdigit() else x
                )

        # === Tambah kolom dapil ===
        df.insert(df.columns.get_loc("nama_lengkap"), "dapil_id", "")

        # === Join pekerjaan ===
        if "pekerjaan" in df.columns:
            df = df.merge(pekerjaan_df, on="pekerjaan", how="left")
        else:
            df["kode_pekerjaan"] = ""

        # === Join partai ===
        if "partai" in df.columns:
            df = df.merge(partai_df, on="partai", how="left")
        else:
            df["kode_partai"] = ""

        # === Join agama ===
        if "agama" in df.columns:
            df = df.merge(agama_df, on="agama", how="left")
        else:
            df["kode_agama"] = ""

        # === Urutkan kolom ===
        order_cols = []
        for col in df.columns:
            if col == "agama":
                order_cols.extend(["agama", "kode_agama"])
            elif col == "provinsi":
                order_cols.extend(["provinsi", "kode_provinsi"])
            elif col == "kabkot":
                order_cols.extend(["kabkot", "kode_kabkot"])
            elif col == "kecamatan":
                order_cols.extend(["kecamatan", "kode_kecamatan"])
            elif col == "deskel":
                order_cols.extend(["deskel", "kode_deskel"])
            elif col == "pekerjaan":
                order_cols.extend(["pekerjaan", "kode_pekerjaan"])
            elif col == "partai":
                order_cols.extend(["partai", "kode_partai"])
            else:
                if col not in ["kode_agama","kode_provinsi","kode_kabkot","kode_kecamatan","kode_deskel","kode_pekerjaan","kode_partai"]:
                    order_cols.append(col)

        df = df[order_cols]

        # === Simpan output ===
        output_filename = os.path.splitext(file.filename)[0] + "_autocode.csv"
        output_path = os.path.join(UPLOAD_FOLDER, output_filename)
        df.to_csv(output_path, index=False)

        # === Kirim preview ke frontend ===
        preview_html = df.head(50).to_html(classes='table table-bordered table-striped', index=False)

        return render_template(
            "index.html",
            message="✅ File berhasil diproses!",
            download_ready=True,
            download_link=f"/download/{output_filename}",
            preview_html=preview_html
        )

    except Exception as e:
        return render_template(
            'index.html',
            message=f'❌ Terjadi error: {e}',
            download_ready=False,
            preview_html=None
        )


@app.route("/download/<filename>")
def download(filename):
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    return render_template("index.html", message="❌ File tidak ditemukan.", download_ready=False)


if __name__ == "__main__":
    app.run()

