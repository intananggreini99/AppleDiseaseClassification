# Apple Leaf Disease Classification — Big Data Stack (Docker)

Sistem analisis klasifikasi penyakit daun apel di atas **Docker** dengan
**HDFS + Apache Spark + Apache Hive + PostgreSQL**. Pipeline melakukan:

1. Ekstraksi citra → vektor fitur numerik (63 fitur: warna RGB/HSV, histogram, tekstur GLCM).
2. Feature selection **RFE** (teroptimasi) pada data train.
3. Penyimpanan hasil preprocessing ke **PostgreSQL**.
4. Pengiriman data dari PostgreSQL ke **HDFS** (format **Parquet**) + registrasi **tabel Hive**.
5. Klasifikasi **Decision Tree (Spark MLlib)** dengan pengukuran durasi train/val/test.
6. Pembandingan durasi **PySpark vs scikit-learn**.

> Panduan lengkap & rinci ada di **`docs/Panduan_Apple_Leaf_Disease_Classification.docx`**.

---

## Prasyarat

- Windows 11 + **Docker Desktop** (backend **WSL2** aktif).
- Alokasi memori Docker **minimal 8 GB** (Settings → Resources).
- Dataset Kaggle: <https://www.kaggle.com/datasets/showravdhar/apple-disease-dataset>

## Struktur dataset (letakkan di folder `data/`)

```
data/
  Apple_Disease_Dataset/
    train/<nama_kelas>/*.jpg
    val/<nama_kelas>/*.jpg      # boleh bernama valid/ atau validation/
    test/<nama_kelas>/*.jpg
```

## Langkah cepat (PowerShell — Windows 11)

```powershell
# 1. Build image + jalankan semua service
.\scripts\01_build_up.ps1

# 2. Cek semua service & Web UI sudah siap
.\scripts\00_check_services.ps1

# 3. Unggah dataset ke HDFS
.\scripts\02_upload_to_hdfs.ps1

# 4. Jalankan seluruh pipeline (Tahap 1-5)
.\scripts\03_run_pipeline.ps1

# 5. (opsional) Hentikan semua container
.\scripts\04_teardown.ps1            # data tetap; -Purge untuk hapus data
```

## Langkah cepat (Git Bash / WSL / Linux / macOS)

```bash
bash scripts/01_build_up.sh
bash scripts/00_check_services.sh
bash scripts/02_upload_to_hdfs.sh
bash scripts/03_run_pipeline.sh
bash scripts/04_teardown.sh          # tambahkan --purge untuk hapus data
```

## Web UI (buka di browser)

| Service              | URL                       |
|----------------------|---------------------------|
| NameNode (HDFS)      | http://localhost:9870     |
| DataNode (HDFS)      | http://localhost:9864     |
| Spark Master         | http://localhost:8080     |
| Spark Worker         | http://localhost:8081     |
| Spark History Server | http://localhost:18080    |
| HiveServer2          | http://localhost:10002    |
| Spark Application UI  | http://localhost:4040 (saat job jalan) |
| PostgreSQL (bukan web)| localhost:5432 (appleuser/applepass, db appledb) |

## Hasil keluaran (folder `output/`)

- `label_mapping.json` — pemetaan label → indeks.
- `selected_features.json` — fitur terpilih hasil RFE.
- `bench_pyspark.json`, `bench_sklearn.json` — durasi & metrik tiap framework.
- `comparison.csv` — tabel perbandingan durasi.
- `comparison_duration.png` — grafik perbandingan durasi.

## Struktur proyek

```
apple-leaf-disease-classification/
├─ docker-compose.yml          # definisi semua service
├─ README.md
├─ config/
│  ├─ hadoop.env               # konfigurasi Hadoop/HDFS/Hive (bde2020)
│  ├─ postgres-init/01_init.sql# inisialisasi schema PostgreSQL
│  └─ spark/                   # core-site, hive-site, spark-defaults
├─ docker/
│  ├─ Dockerfile.spark         # image Spark kustom (Spark 3.3.2 + Python ML)
│  └─ requirements.txt
├─ src/                        # kode pipeline (5 tahap) + config/util
│  ├─ config.py  utils.py
│  ├─ step1_extract_features.py
│  ├─ step2_postgres_to_hive.py
│  ├─ step3_train_spark_dt.py
│  ├─ step4_train_sklearn_dt.py
│  └─ step5_compare_results.py
├─ scripts/                    # skrip .sh (Bash) & .ps1 (PowerShell)
├─ data/                       # (Anda isi) dataset Kaggle
├─ output/                     # hasil pipeline
└─ docs/                       # panduan Word (.docx)
```

## Catatan

- **Spark MLlib** dan **scikit-learn** Decision Tree bukan algoritma yang identik;
  hasil perbandingan durasi bersifat **indikatif**.
- Jalankan **Tahap 1–5 berurutan**; tiap tahap bergantung pada output tahap sebelumnya.
- Lihat bagian **Troubleshooting** pada dokumen Word jika ada kendala.
