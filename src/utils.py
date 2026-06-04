"""
utils.py
========
Fungsi pembantu yang dipakai lintas pipeline:
  - feature_names()            : daftar nama fitur (urut, deterministik)
  - extract_features(bytes)    : ekstraksi citra -> vektor fitur numerik
  - Timer                      : context manager untuk mengukur durasi
  - log_stage(...)             : mencatat durasi tahap ke PostgreSQL
  - build_spark(...)           : membuat SparkSession dengan Hive aktif
  - label_from_path(path)      : ambil label kelas dari path file
"""
import io
import time
import contextlib

import numpy as np

import config


# =====================================================================
# 1. DEFINISI FITUR
# =====================================================================
# Vektor fitur disusun dengan urutan tetap supaya kolom di PostgreSQL,
# Parquet, dan Hive konsisten. Total panjang vektor = 63 (default param).
#
#   - Statistik per-kanal RGB  : 3 kanal x 7 statistik          = 21
#   - Statistik per-kanal HSV  : 3 kanal x 2 statistik          =  6
#   - Histogram warna RGB      : 3 kanal x HIST_BINS(8)         = 24
#   - Fitur tekstur GLCM       : 6 properti x 2 (mean,std)      = 12
# ---------------------------------------------------------------------
_RGB_STATS = ["mean", "std", "min", "max", "p25", "p50", "p75"]
_HSV_STATS = ["mean", "std"]
_GLCM_PROPS = ["contrast", "dissimilarity", "homogeneity",
               "energy", "correlation", "ASM"]


def feature_names():
    """Mengembalikan list nama fitur sesuai urutan vektor ekstraksi."""
    names = []
    for ch in ["R", "G", "B"]:
        for s in _RGB_STATS:
            names.append(f"rgb_{ch}_{s}")
    for ch in ["H", "S", "V"]:
        for s in _HSV_STATS:
            names.append(f"hsv_{ch}_{s}")
    for ch in ["R", "G", "B"]:
        for b in range(config.HIST_BINS):
            names.append(f"hist_{ch}_{b}")
    for p in _GLCM_PROPS:
        names.append(f"glcm_{p}_mean")
        names.append(f"glcm_{p}_std")
    return names


N_FEATURES = len(feature_names())


# =====================================================================
# 2. EKSTRAKSI FITUR CITRA -> VEKTOR NUMERIK
# =====================================================================
def extract_features(image_bytes):
    """
    Mengubah byte citra (JPG/PNG) menjadi vektor fitur numerik (list float).

    Mengembalikan list[float] sepanjang N_FEATURES, atau None bila gambar
    gagal didekode (akan difilter di pipeline).
    """
    # Import di dalam fungsi agar aman saat dijalankan sebagai UDF Spark
    # (modul tersedia di setiap executor).
    from PIL import Image
    from skimage.feature import graycomatrix, graycoprops
    from skimage.color import rgb2gray

    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    except Exception:
        return None

    size = config.IMAGE_SIZE
    img = img.resize((size, size))
    arr = np.asarray(img, dtype=np.float64)        # (H, W, 3)

    feats = []

    # --- Statistik per-kanal RGB ---
    for c in range(3):
        ch = arr[:, :, c]
        feats.extend([
            float(ch.mean()),
            float(ch.std()),
            float(ch.min()),
            float(ch.max()),
            float(np.percentile(ch, 25)),
            float(np.percentile(ch, 50)),
            float(np.percentile(ch, 75)),
        ])

    # --- Statistik per-kanal HSV ---
    hsv = np.asarray(img.convert("HSV"), dtype=np.float64)
    for c in range(3):
        ch = hsv[:, :, c]
        feats.extend([float(ch.mean()), float(ch.std())])

    # --- Histogram warna RGB (ternormalisasi) ---
    bins = config.HIST_BINS
    for c in range(3):
        hist, _ = np.histogram(arr[:, :, c], bins=bins, range=(0, 256))
        total = hist.sum()
        hist = hist / total if total > 0 else hist
        feats.extend([float(x) for x in hist])

    # --- Fitur tekstur GLCM (grayscale terkuantisasi) ---
    levels = config.GLCM_LEVELS
    gray = (rgb2gray(arr / 255.0) * 255.0).astype(np.uint8)
    # Kuantisasi level abu-abu untuk mempercepat GLCM
    q = (gray.astype(np.int32) * levels // 256).astype(np.uint8)
    q[q >= levels] = levels - 1
    glcm = graycomatrix(
        q,
        distances=[1, 2],
        angles=[0, np.pi / 4, np.pi / 2, 3 * np.pi / 4],
        levels=levels,
        symmetric=True,
        normed=True,
    )
    for prop in _GLCM_PROPS:
        vals = graycoprops(glcm, prop)
        feats.extend([float(vals.mean()), float(vals.std())])

    # Pengaman: pastikan panjang vektor konsisten
    if len(feats) != N_FEATURES:
        return None
    return feats


# =====================================================================
# 3. UTILITAS PATH / LABEL
# =====================================================================
def label_from_path(path):
    """
    Mengambil nama kelas dari path file citra.
    Struktur diasumsikan: .../<split>/<label>/<file>.jpg
    """
    parts = path.replace("\\", "/").rstrip("/").split("/")
    if len(parts) >= 2:
        return parts[-2]
    return "unknown"


def split_from_path(path):
    """Mengambil nama split (train/val/test) dari path file citra."""
    p = path.replace("\\", "/")
    for s in config.SPLITS:
        if f"/{s}/" in p:
            return s
    return "unknown"


# =====================================================================
# 4. PENGUKUR DURASI
# =====================================================================
class Timer(contextlib.AbstractContextManager):
    """
    Context manager pengukur durasi.

        with Timer() as t:
            ...kerja...
        print(t.elapsed)   # detik (float)
    """
    def __init__(self, label=""):
        self.label = label
        self.elapsed = 0.0
        self._start = None

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.elapsed = time.perf_counter() - self._start
        if self.label:
            print(f"[TIMER] {self.label}: {self.elapsed:.4f} detik")
        return False


# =====================================================================
# 5. LOGGING TAHAP -> PostgreSQL
# =====================================================================
def log_stage(stage, message="", duration_s=None):
    """
    Mencatat durasi/keterangan sebuah tahap pipeline ke tabel
    apple.pipeline_log (lewat psycopg2). Aman bila gagal (hanya warning).
    """
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=config.PG_HOST,
            port=config.PG_PORT,
            dbname=config.PG_DB,
            user=config.PG_USER,
            password=config.PG_PASSWORD,
        )
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO apple.pipeline_log (stage, message, duration_s) "
                "VALUES (%s, %s, %s)",
                (stage, message, duration_s),
            )
        conn.close()
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] Gagal menulis pipeline_log ({stage}): {exc}")


# =====================================================================
# 6. SPARK SESSION
# =====================================================================
def build_spark(app_name, enable_hive=True):
    """
    Membuat SparkSession terhubung ke cluster standalone dengan Hive aktif.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir",
                f"{config.HDFS_NAMENODE}/user/hive/warehouse")
        .config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE)
    )
    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def print_header(title):
    """Mencetak header agar log mudah dibaca."""
    line = "=" * 70
    print(f"\n{line}\n  {title}\n{line}")
