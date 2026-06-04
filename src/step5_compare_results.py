"""
step5_compare_results.py
========================
TAHAP 5 PIPELINE - Perbandingan PySpark (Spark MLlib) vs scikit-learn.

Membaca hasil benchmark kedua framework (output/bench_pyspark.json &
output/bench_sklearn.json), lalu:
  1. Menyusun tabel perbandingan durasi & metrik per tahap.
  2. Menghitung total durasi (training + evaluasi) tiap framework.
  3. Menyimpan ringkasan ke CSV, grafik PNG, dan tabel PostgreSQL
     (apple.comparison_summary).

Dijalankan dengan `python3` biasa di dalam container spark-master.
"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

import config
import utils

import matplotlib
matplotlib.use("Agg")  # backend non-interaktif (tanpa display)
import matplotlib.pyplot as plt


STAGES = ["train", "validation", "test"]


def load(framework):
    path = os.path.join(config.OUTPUT_DIR, f"bench_{framework}.json")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{path} tidak ditemukan. Jalankan tahap "
            f"{'3' if framework == 'pyspark' else '4'} terlebih dahulu."
        )
    with open(path) as f:
        return {r["stage"]: r for r in json.load(f)}


def main():
    ps = load("pyspark")
    sk = load("sklearn")

    # ---------------------------------------------------------------
    # 1. Tabel perbandingan per tahap
    # ---------------------------------------------------------------
    rows = []
    for stage in STAGES:
        p, s = ps.get(stage, {}), sk.get(stage, {})
        pd_dur = p.get("duration_s", float("nan"))
        sd_dur = s.get("duration_s", float("nan"))
        rows.append({
            "stage": stage,
            "pyspark_duration_s": pd_dur,
            "sklearn_duration_s": sd_dur,
            "speedup_sklearn_vs_pyspark": (pd_dur / sd_dur)
                if sd_dur not in (0, None) else float("nan"),
            "pyspark_accuracy": p.get("accuracy"),
            "sklearn_accuracy": s.get("accuracy"),
            "pyspark_f1": p.get("f1"),
            "sklearn_f1": s.get("f1"),
        })
    comp = pd.DataFrame(rows)

    # Baris total (training + evaluasi)
    total_ps = sum(ps[s]["duration_s"] for s in STAGES if s in ps)
    total_sk = sum(sk[s]["duration_s"] for s in STAGES if s in sk)
    comp_total = pd.DataFrame([{
        "stage": "TOTAL (train+val+test)",
        "pyspark_duration_s": total_ps,
        "sklearn_duration_s": total_sk,
        "speedup_sklearn_vs_pyspark": (total_ps / total_sk)
            if total_sk else float("nan"),
        "pyspark_accuracy": None, "sklearn_accuracy": None,
        "pyspark_f1": None, "sklearn_f1": None,
    }])
    comp_full = pd.concat([comp, comp_total], ignore_index=True)

    utils.print_header("PERBANDINGAN DURASI & METRIK: PySpark vs scikit-learn")
    with pd.option_context("display.float_format", lambda v: f"{v:.6f}"):
        print(comp_full.to_string(index=False))

    print(f"\nTotal durasi (train+val+test):")
    print(f"  PySpark (Spark MLlib) : {total_ps:.4f} detik")
    print(f"  scikit-learn          : {total_sk:.6f} detik")
    if total_sk:
        print(f"  Rasio PySpark/scikit  : {total_ps / total_sk:.2f}x")

    # ---------------------------------------------------------------
    # 2. Simpan CSV
    # ---------------------------------------------------------------
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(config.OUTPUT_DIR, "comparison.csv")
    comp_full.to_csv(csv_path, index=False)
    print(f"\nTabel perbandingan disimpan: {csv_path}")

    # ---------------------------------------------------------------
    # 3. Grafik batang durasi per tahap
    # ---------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(8, 5))
    x = range(len(STAGES))
    w = 0.35
    ax.bar([i - w / 2 for i in x], [ps[s]["duration_s"] for s in STAGES],
           width=w, label="PySpark (MLlib)")
    ax.bar([i + w / 2 for i in x], [sk[s]["duration_s"] for s in STAGES],
           width=w, label="scikit-learn")
    ax.set_xticks(list(x))
    ax.set_xticklabels(STAGES)
    ax.set_ylabel("Durasi (detik)")
    ax.set_title("Durasi Decision Tree per Tahap: PySpark vs scikit-learn")
    ax.legend()
    fig.tight_layout()
    png_path = os.path.join(config.OUTPUT_DIR, "comparison_duration.png")
    fig.savefig(png_path, dpi=120)
    print(f"Grafik disimpan: {png_path}")

    # ---------------------------------------------------------------
    # 4. Tulis ringkasan ke PostgreSQL
    # ---------------------------------------------------------------
    try:
        from sqlalchemy import create_engine
        engine = create_engine(config.PG_SQLALCHEMY_URL)
        comp_full.to_sql(
            "comparison_summary", engine, schema=config.PG_SCHEMA,
            if_exists="replace", index=False,
        )
        print(f"Ringkasan ditulis ke {config.PG_SCHEMA}.comparison_summary")
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] Gagal menulis comparison_summary: {exc}")

    utils.print_header("SELESAI - seluruh pipeline tuntas")


if __name__ == "__main__":
    main()
