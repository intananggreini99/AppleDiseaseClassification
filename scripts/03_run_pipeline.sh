#!/usr/bin/env bash
# =====================================================================
# 03_run_pipeline.sh
# Menjalankan seluruh tahap pipeline secara berurutan di dalam
# container spark-master.
#
#   TAHAP 1 : Ekstraksi fitur + RFE  -> simpan ke PostgreSQL
#   TAHAP 2 : PostgreSQL -> Parquet di HDFS + tabel Hive
#   TAHAP 3 : Decision Tree (Spark MLlib) + durasi train/val/test
#   TAHAP 4 : Decision Tree (scikit-learn) + durasi train/val/test
#   TAHAP 5 : Perbandingan PySpark vs scikit-learn
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

# Argumen spark-submit yang dipakai bersama.
# --py-files mengirim config.py & utils.py ke executor (dibutuhkan UDF).
SUBMIT="/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.6.0.jar \
  --py-files /app/src/config.py,/app/src/utils.py"

run_spark() {  # $1 = path script
  echo
  echo "==================================================================="
  echo ">>> spark-submit $1"
  echo "==================================================================="
  docker exec spark-master bash -lc "${SUBMIT} $1"
}

run_python() {  # $1 = path script (tanpa Spark)
  echo
  echo "==================================================================="
  echo ">>> python3 $1"
  echo "==================================================================="
  docker exec spark-master bash -lc "python3 $1"
}

run_spark  /app/src/step1_extract_features.py
run_spark  /app/src/step2_postgres_to_hive.py
run_spark  /app/src/step3_train_spark_dt.py
run_python /app/src/step4_train_sklearn_dt.py
run_python /app/src/step5_compare_results.py

echo
echo ">>> Pipeline selesai."
echo ">>> Hasil ada di folder ./output (label_mapping.json, selected_features.json,"
echo "    bench_pyspark.json, bench_sklearn.json, comparison.csv, comparison_duration.png)"
