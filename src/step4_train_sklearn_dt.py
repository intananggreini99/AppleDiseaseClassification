"""
step4_train_sklearn_dt.py
=========================
TAHAP 4 PIPELINE - Klasifikasi Decision Tree dengan scikit-learn.

Tujuan: PEMBANDING terhadap Spark MLlib. Menggunakan data preprocessing
yang sama (dibaca dari PostgreSQL apple.features) dan parameter pohon
yang dibuat semirip mungkin, lalu mengukur durasi training/validation/test.

Skrip ini TIDAK memerlukan cluster Spark (cukup pandas + scikit-learn),
sehingga dijalankan dengan `python3` biasa di dalam container spark-master.
"""
import os
import sys
import json
import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd

import config
import utils

from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (
    accuracy_score, f1_score, precision_score, recall_score,
)

META_COLS = ["label", "label_index", "split", "source_path"]
FRAMEWORK = "sklearn"


def metrics_dict(y_true, y_pred):
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "f1": float(f1_score(y_true, y_pred,
                             average="weighted", zero_division=0)),
        "weighted_precision": float(precision_score(
            y_true, y_pred, average="weighted", zero_division=0)),
        "weighted_recall": float(recall_score(
            y_true, y_pred, average="weighted", zero_division=0)),
    }


def main():
    run_ts = datetime.datetime.now().isoformat(timespec="seconds")
    from sqlalchemy import create_engine
    engine = create_engine(config.PG_SQLALCHEMY_URL)

    # ---------------------------------------------------------------
    # 1. Muat data dari PostgreSQL
    # ---------------------------------------------------------------
    utils.print_header(f"Memuat data dari PostgreSQL: {config.TBL_FEATURES}")
    df = pd.read_sql_query(f"SELECT * FROM {config.TBL_FEATURES}", engine)
    feature_cols = [c for c in df.columns if c not in META_COLS]
    print(f"  -> {len(feature_cols)} kolom fitur, total {len(df)} baris")

    def xy(split):
        sub = df[df["split"] == split]
        return (sub[feature_cols].to_numpy(dtype=np.float64),
                sub["label_index"].to_numpy(dtype=int))

    x_train, y_train = xy("train")
    x_val, y_val = xy("val")
    x_test, y_test = xy("test")
    print(f"  -> train={len(y_train)}, val={len(y_val)}, test={len(y_test)}")

    # ---------------------------------------------------------------
    # 2. Training (ukur durasi)
    # ---------------------------------------------------------------
    utils.print_header("Training DecisionTree (scikit-learn)")
    clf = DecisionTreeClassifier(
        max_depth=config.DT_MAX_DEPTH,
        min_samples_leaf=config.DT_MIN_INSTANCES,
        random_state=config.RANDOM_SEED,
    )
    with utils.Timer("scikit-learn - training") as t_train:
        clf.fit(x_train, y_train)
    print(f"  Kedalaman pohon: {clf.get_depth()}, "
          f"jumlah leaf: {clf.get_n_leaves()}")

    # ---------------------------------------------------------------
    # 3. Evaluasi train/val/test (ukur durasi)
    # ---------------------------------------------------------------
    results = []

    train_metrics = metrics_dict(y_train, clf.predict(x_train))
    results.append({"stage": "train", "duration_s": t_train.elapsed,
                    "n_samples": int(len(y_train)), **train_metrics})

    utils.print_header("Evaluasi VALIDATION (scikit-learn)")
    with utils.Timer("scikit-learn - validation") as t_val:
        val_pred = clf.predict(x_val)
        val_metrics = metrics_dict(y_val, val_pred)
    results.append({"stage": "validation", "duration_s": t_val.elapsed,
                    "n_samples": int(len(y_val)), **val_metrics})

    utils.print_header("Evaluasi TEST (scikit-learn)")
    with utils.Timer("scikit-learn - testing") as t_test:
        test_pred = clf.predict(x_test)
        test_metrics = metrics_dict(y_test, test_pred)
    results.append({"stage": "test", "duration_s": t_test.elapsed,
                    "n_samples": int(len(y_test)), **test_metrics})

    # ---------------------------------------------------------------
    # 4. Ringkasan
    # ---------------------------------------------------------------
    utils.print_header("RINGKASAN scikit-learn Decision Tree")
    for r in results:
        print(f"  {r['stage']:<11} | durasi={r['duration_s']:.6f}s "
              f"| acc={r['accuracy']:.4f} | f1={r['f1']:.4f}")

    # ---------------------------------------------------------------
    # 5. Simpan hasil (JSON + PostgreSQL)
    # ---------------------------------------------------------------
    for r in results:
        r["framework"] = FRAMEWORK
        r["run_ts"] = run_ts

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    out_json = os.path.join(config.OUTPUT_DIR, "bench_sklearn.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nHasil ditulis ke {out_json}")

    cols = ["framework", "stage", "duration_s", "accuracy", "f1",
            "weighted_precision", "weighted_recall", "n_samples", "run_ts"]
    bench_df = pd.DataFrame(results)[cols]
    bench_df.to_sql(
        "benchmark_results", engine, schema=config.PG_SCHEMA,
        if_exists="append", index=False,
    )
    print(f"Hasil di-append ke {config.TBL_BENCHMARK}")


if __name__ == "__main__":
    main()
