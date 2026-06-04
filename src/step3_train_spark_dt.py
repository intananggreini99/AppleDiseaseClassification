"""
step3_train_spark_dt.py
=======================
TAHAP 3 PIPELINE - Klasifikasi Decision Tree dengan Spark MLlib.

Alur:
  1. Memuat data hasil preprocessing dari tabel Hive (apple_db.features).
  2. Merakit kolom fitur menjadi satu vektor (VectorAssembler).
  3. Melatih DecisionTreeClassifier (Spark MLlib) pada split TRAIN dan
     MENGUKUR DURASI training.
  4. Mengevaluasi & MENGUKUR DURASI pada split VALIDATION dan TEST
     (accuracy, F1, precision, recall).
  5. Menyimpan hasil (durasi + metrik) ke PostgreSQL (apple.benchmark_results)
     dan ke file JSON (output/bench_pyspark.json).

Dijalankan via spark-submit di dalam container spark-master.
"""
import os
import sys
import json
import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import utils

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


META_COLS = ["label", "label_index", "split", "source_path"]
FRAMEWORK = "pyspark"


def evaluate(predictions):
    """Menghitung metrik multikelas dari kolom prediksi & label_index."""
    metrics = {}
    for name, metric in [
        ("accuracy", "accuracy"),
        ("f1", "f1"),
        ("weighted_precision", "weightedPrecision"),
        ("weighted_recall", "weightedRecall"),
    ]:
        ev = MulticlassClassificationEvaluator(
            labelCol="label_index",
            predictionCol="prediction",
            metricName=metric,
        )
        metrics[name] = float(ev.evaluate(predictions))
    return metrics


def main():
    spark = utils.build_spark("03-Train-SparkMLlib-DT")
    run_ts = datetime.datetime.now().isoformat(timespec="seconds")

    # ---------------------------------------------------------------
    # 1. Muat data dari Hive
    # ---------------------------------------------------------------
    utils.print_header(f"Memuat data dari Hive: {config.HIVE_TABLE_FEATURES}")
    df = spark.table(config.HIVE_TABLE_FEATURES)
    feature_cols = [c for c in df.columns if c not in META_COLS]
    print(f"  -> {len(feature_cols)} kolom fitur, total {df.count()} baris")

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="keep"
    )
    data = assembler.transform(df).select("features", "label_index", "split")
    data.cache()

    train = data.filter("split = 'train'")
    val = data.filter("split = 'val'")
    test = data.filter("split = 'test'")
    n_train, n_val, n_test = train.count(), val.count(), test.count()
    print(f"  -> train={n_train}, val={n_val}, test={n_test}")

    # ---------------------------------------------------------------
    # 2. Training (ukur durasi)
    # ---------------------------------------------------------------
    utils.print_header("Training DecisionTree (Spark MLlib)")
    dt = DecisionTreeClassifier(
        featuresCol="features",
        labelCol="label_index",
        maxDepth=config.DT_MAX_DEPTH,
        minInstancesPerNode=config.DT_MIN_INSTANCES,
        maxBins=config.DT_MAX_BINS,
        seed=config.RANDOM_SEED,
    )
    with utils.Timer("PySpark - training") as t_train:
        model = dt.fit(train)
    print(f"  Kedalaman pohon: {model.depth}, jumlah node: {model.numNodes}")

    # ---------------------------------------------------------------
    # 3. Evaluasi train/val/test (ukur durasi tiap tahap)
    # ---------------------------------------------------------------
    results = []

    # -- train (metrik atas data latih; durasi = waktu training) --
    train_pred = model.transform(train)
    train_metrics = evaluate(train_pred)
    results.append({"stage": "train", "duration_s": t_train.elapsed,
                    "n_samples": n_train, **train_metrics})

    # -- validation --
    utils.print_header("Evaluasi VALIDATION (Spark MLlib)")
    with utils.Timer("PySpark - validation") as t_val:
        val_pred = model.transform(val)
        val_metrics = evaluate(val_pred)
    results.append({"stage": "validation", "duration_s": t_val.elapsed,
                    "n_samples": n_val, **val_metrics})

    # -- test --
    utils.print_header("Evaluasi TEST (Spark MLlib)")
    with utils.Timer("PySpark - testing") as t_test:
        test_pred = model.transform(test)
        test_metrics = evaluate(test_pred)
    results.append({"stage": "test", "duration_s": t_test.elapsed,
                    "n_samples": n_test, **test_metrics})

    # ---------------------------------------------------------------
    # 4. Cetak ringkasan
    # ---------------------------------------------------------------
    utils.print_header("RINGKASAN PySpark Decision Tree")
    for r in results:
        print(f"  {r['stage']:<11} | durasi={r['duration_s']:.4f}s "
              f"| acc={r['accuracy']:.4f} | f1={r['f1']:.4f}")

    # ---------------------------------------------------------------
    # 5. Simpan hasil (JSON + PostgreSQL)
    # ---------------------------------------------------------------
    for r in results:
        r["framework"] = FRAMEWORK
        r["run_ts"] = run_ts

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    out_json = os.path.join(config.OUTPUT_DIR, "bench_pyspark.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nHasil ditulis ke {out_json}")

    # Tulis ke PostgreSQL (append; tabel dibuat bila belum ada)
    cols = ["framework", "stage", "duration_s", "accuracy", "f1",
            "weighted_precision", "weighted_recall", "n_samples", "run_ts"]
    rows = [tuple(r[c] for c in cols) for r in results]
    bench_df = spark.createDataFrame(rows, schema=cols)
    (bench_df.write
        .mode("append")
        .jdbc(config.PG_JDBC_URL, config.TBL_BENCHMARK,
              properties=config.PG_JDBC_PROPERTIES))
    print(f"Hasil di-append ke {config.TBL_BENCHMARK}")

    spark.stop()


if __name__ == "__main__":
    main()
