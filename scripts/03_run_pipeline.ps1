# =====================================================================
# 03_run_pipeline.ps1
# Menjalankan seluruh tahap pipeline secara berurutan di dalam
# container spark-master.
#
#   TAHAP 1 : Ekstraksi fitur + RFE  -> simpan ke PostgreSQL
#   TAHAP 2 : PostgreSQL -> Parquet di HDFS + tabel Hive
#   TAHAP 3 : Decision Tree (Spark MLlib) + durasi train/val/test
#   TAHAP 4 : Decision Tree (scikit-learn) + durasi train/val/test
#   TAHAP 5 : Perbandingan PySpark vs scikit-learn
#
#   PS> .\scripts\03_run_pipeline.ps1
# =====================================================================
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

# Argumen spark-submit yang dipakai bersama.
$SUBMIT = "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.6.0.jar --py-files /app/src/config.py,/app/src/utils.py"

function Run-Spark($script) {
    Write-Host ""
    Write-Host "===================================================================" -ForegroundColor Cyan
    Write-Host ">>> spark-submit $script" -ForegroundColor Cyan
    Write-Host "===================================================================" -ForegroundColor Cyan
    docker exec spark-master bash -lc "$SUBMIT $script"
    if ($LASTEXITCODE -ne 0) { throw "Gagal menjalankan $script" }
}

function Run-Python($script) {
    Write-Host ""
    Write-Host "===================================================================" -ForegroundColor Cyan
    Write-Host ">>> python3 $script" -ForegroundColor Cyan
    Write-Host "===================================================================" -ForegroundColor Cyan
    docker exec spark-master bash -lc "python3 $script"
    if ($LASTEXITCODE -ne 0) { throw "Gagal menjalankan $script" }
}

Run-Spark  "/app/src/step1_extract_features.py"
Run-Spark  "/app/src/step2_postgres_to_hive.py"
Run-Spark  "/app/src/step3_train_spark_dt.py"
Run-Python "/app/src/step4_train_sklearn_dt.py"
Run-Python "/app/src/step5_compare_results.py"

Write-Host ""
Write-Host ">>> Pipeline selesai." -ForegroundColor Green
Write-Host ">>> Hasil ada di folder .\output (label_mapping.json, selected_features.json,"
Write-Host "    bench_pyspark.json, bench_sklearn.json, comparison.csv, comparison_duration.png)"
