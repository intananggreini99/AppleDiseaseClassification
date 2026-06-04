"""
step1_extract_features.py
=========================
TAHAP 1 PIPELINE - Ekstraksi fitur + Feature Selection (RFE).

Alur:
  1. Membaca citra (train/val/test) dari HDFS memakai reader binaryFile
     Spark (terdistribusi ke worker).
  2. Mengekstraksi setiap citra menjadi vektor fitur numerik (UDF Spark)
     -> warna RGB/HSV, histogram, dan tekstur GLCM. Total 63 fitur.
  3. Menjalankan RFE (Recursive Feature Elimination) yang teroptimasi
     pada data TRAIN saja (step>1 + base estimator dangkal) untuk memilih
     subset fitur paling informatif.
  4. Menyimpan vektor fitur terpilih untuk seluruh split ke PostgreSQL
     (tabel apple.features) -> ini adalah data hasil preprocessing.

Dijalankan via spark-submit di dalam container spark-master.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd

import config
import utils

from pyspark.sql.functions import udf, col, lower
from pyspark.sql.types import ArrayType, DoubleType


# ---------------------------------------------------------------------
# Kolom metadata (bukan fitur) pada tabel hasil preprocessing
# ---------------------------------------------------------------------
META_COLS = ["label", "label_index", "split", "source_path"]


def read_split_features(spark, split):
    """
    Membaca seluruh citra pada satu split dari HDFS, mengekstraksi fitur
    di executor (UDF), lalu mengumpulkan (path, features) ke driver.
    """
    path = f"{config.HDFS_IMAGE_BASE}/{split}"
    utils.print_header(f"Ekstraksi fitur split '{split}' dari {path}")

    raw = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(path)
    )
    # Saring hanya berkas citra
    raw = raw.filter(lower(col("path")).rlike(r"\.(jpg|jpeg|png|bmp)$"))

    extract_udf = udf(utils.extract_features, ArrayType(DoubleType()))
    feat_df = (
        raw.withColumn("features", extract_udf(col("content")))
        .select("path", "features")
        .filter(col("features").isNotNull())
    )

    # Kumpulkan ke driver (hanya angka + path, ukuran kecil)
    rows = feat_df.collect()
    print(f"  -> {len(rows)} citra berhasil diproses pada split '{split}'")

    pdf = pd.DataFrame({
        "source_path": [r["path"] for r in rows],
        "features": [list(r["features"]) for r in rows],
    })
    pdf["split"] = split
    pdf["label"] = pdf["source_path"].apply(utils.label_from_path)
    return pdf


def run_rfe(x_train, y_train):
    """RFE teroptimasi: estimator dangkal + step>1 -> sedikit iterasi."""
    from sklearn.feature_selection import RFE
    from sklearn.tree import DecisionTreeClassifier

    n_total = x_train.shape[1]
    n_select = min(config.RFE_N_FEATURES, n_total)

    base = DecisionTreeClassifier(
        max_depth=config.RFE_BASE_MAX_DEPTH,
        random_state=config.RANDOM_SEED,
    )
    rfe = RFE(
        estimator=base,
        n_features_to_select=n_select,
        step=config.RFE_STEP,
    )
    utils.print_header(
        f"RFE: pilih {n_select}/{n_total} fitur (step={config.RFE_STEP})"
    )
    with utils.Timer("RFE fit") as t:
        rfe.fit(x_train, y_train)
    utils.log_stage("rfe_fit",
                    f"select {n_select}/{n_total}", t.elapsed)
    return rfe.support_, t.elapsed


def main():
    spark = utils.build_spark("01-ExtractFeatures-RFE")

    with utils.Timer("Total ekstraksi fitur semua split") as t_ext:
        frames = [read_split_features(spark, s) for s in config.SPLITS]
    utils.log_stage("feature_extraction",
                    f"{sum(len(f) for f in frames)} citra", t_ext.elapsed)

    all_pdf = pd.concat(frames, ignore_index=True)
    if all_pdf.empty:
        raise RuntimeError(
            "Tidak ada citra terbaca. Pastikan dataset sudah diunggah ke HDFS "
            f"pada {config.HDFS_IMAGE_BASE}/<split>/<label>/*.jpg"
        )

    # Matriks fitur & label
    feat_names = utils.feature_names()
    x_all = np.asarray(all_pdf["features"].tolist(), dtype=np.float64)

    labels_sorted = sorted(all_pdf["label"].unique())
    label_to_idx = {lab: i for i, lab in enumerate(labels_sorted)}
    y_all = all_pdf["label"].map(label_to_idx).to_numpy()
    split_arr = all_pdf["split"].to_numpy()

    print("\nDistribusi label:")
    print(all_pdf.groupby(["split", "label"]).size())

    # RFE hanya di data train
    train_mask = split_arr == "train"
    if train_mask.sum() == 0:
        raise RuntimeError("Split 'train' kosong; RFE tidak bisa dijalankan.")
    support, _ = run_rfe(x_all[train_mask], y_all[train_mask])
    sel_idx = np.where(support)[0]
    sel_names = [feat_names[i] for i in sel_idx]
    print(f"\nFitur terpilih ({len(sel_names)}): {sel_names}")

    # Susun tabel hasil preprocessing (fitur terpilih + metadata)
    out = pd.DataFrame(x_all[:, sel_idx], columns=sel_names)
    out["label"] = all_pdf["label"].to_numpy()
    out["label_index"] = y_all.astype(int)
    out["split"] = split_arr
    out["source_path"] = all_pdf["source_path"].to_numpy()

    # Simpan mapping label & fitur terpilih ke file output (untuk dokumentasi)
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    pd.Series(label_to_idx).to_json(
        os.path.join(config.OUTPUT_DIR, "label_mapping.json"))
    pd.Series(sel_names).to_json(
        os.path.join(config.OUTPUT_DIR, "selected_features.json"))

    # Tulis ke PostgreSQL (mode overwrite -> tabel dibuat ulang)
    utils.print_header(f"Menyimpan hasil preprocessing ke {config.TBL_FEATURES}")
    sdf = spark.createDataFrame(out)
    with utils.Timer("Tulis ke PostgreSQL") as t_w:
        (sdf.write
            .mode("overwrite")
            .option("truncate", "false")
            .jdbc(config.PG_JDBC_URL, config.TBL_FEATURES,
                  properties=config.PG_JDBC_PROPERTIES))
    utils.log_stage("write_postgres",
                    f"{out.shape[0]} baris x {out.shape[1]} kolom", t_w.elapsed)

    print(f"\nSelesai. {out.shape[0]} baris tersimpan di {config.TBL_FEATURES}.")
    spark.stop()


if __name__ == "__main__":
    main()
