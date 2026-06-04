"""
config.py
=========
Konfigurasi terpusat untuk seluruh skrip pipeline Apple Leaf Disease
Classification. Semua nilai dapat di-override melalui environment variable
sehingga mudah disesuaikan tanpa mengubah kode.
"""
import os

# ---------------------------------------------------------------------
# HDFS
# ---------------------------------------------------------------------
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")

# Lokasi citra mentah di HDFS (hasil unggah dari ./data)
HDFS_IMAGE_BASE = os.getenv("HDFS_IMAGE_BASE", f"{HDFS_NAMENODE}/data/apple")
# Lokasi Parquet hasil preprocessing di HDFS
HDFS_PARQUET_PATH = os.getenv(
    "HDFS_PARQUET_PATH", f"{HDFS_NAMENODE}/data/apple/parquet/features"
)

# Daftar split dataset
SPLITS = ["train", "val", "test"]

# ---------------------------------------------------------------------
# PostgreSQL (database aplikasi)
# ---------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "appledb")
PG_USER = os.getenv("PG_USER", "appleuser")
PG_PASSWORD = os.getenv("PG_PASSWORD", "applepass")

PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_JDBC_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# DSN untuk psycopg2 / SQLAlchemy
PG_SQLALCHEMY_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# Nama tabel di PostgreSQL (schema "apple")
PG_SCHEMA = "apple"
TBL_FEATURES = f"{PG_SCHEMA}.features"             # fitur hasil preprocessing + RFE
TBL_BENCHMARK = f"{PG_SCHEMA}.benchmark_results"   # hasil benchmark durasi & metrik

# ---------------------------------------------------------------------
# Hive
# ---------------------------------------------------------------------
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "apple_db")
HIVE_TABLE_FEATURES = f"{HIVE_DATABASE}.features"

# ---------------------------------------------------------------------
# Parameter ekstraksi fitur citra
# ---------------------------------------------------------------------
IMAGE_SIZE = int(os.getenv("IMAGE_SIZE", "128"))       # ukuran resize (px) sisi
GLCM_LEVELS = int(os.getenv("GLCM_LEVELS", "8"))       # jumlah level abu-abu GLCM
HIST_BINS = int(os.getenv("HIST_BINS", "8"))           # jumlah bin histogram warna

# ---------------------------------------------------------------------
# Parameter RFE (feature selection)
# ---------------------------------------------------------------------
RFE_N_FEATURES = int(os.getenv("RFE_N_FEATURES", "30"))  # jumlah fitur dipertahankan
RFE_STEP = int(os.getenv("RFE_STEP", "5"))               # fitur dibuang per iterasi
RFE_BASE_MAX_DEPTH = int(os.getenv("RFE_BASE_MAX_DEPTH", "8"))

# ---------------------------------------------------------------------
# Parameter model Decision Tree
# ---------------------------------------------------------------------
DT_MAX_DEPTH = int(os.getenv("DT_MAX_DEPTH", "10"))
DT_MIN_INSTANCES = int(os.getenv("DT_MIN_INSTANCES", "2"))   # minInstancesPerNode (Spark)
DT_MAX_BINS = int(os.getenv("DT_MAX_BINS", "32"))            # maxBins (Spark)
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))

# ---------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

# ---------------------------------------------------------------------
# Output lokal (di-mount dari host ./output)
# ---------------------------------------------------------------------
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")
