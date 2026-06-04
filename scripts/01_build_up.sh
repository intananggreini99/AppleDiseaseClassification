#!/usr/bin/env bash
# =====================================================================
# 01_build_up.sh
# Membangun image Spark kustom dan menjalankan seluruh service Docker.
# Jalankan dari Git Bash / WSL pada Windows 11 (atau Linux/macOS).
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">>> [1/3] Membangun image Spark kustom (apple-spark:3.3.2)..."
docker compose build

echo ">>> [2/3] Menjalankan seluruh service (mode detached)..."
docker compose up -d

echo ">>> [3/3] Status container:"
docker compose ps

cat <<'EOF'

------------------------------------------------------------------
Catatan:
- Pertama kali dijalankan, image base (Hadoop/Hive/Spark) akan
  diunduh sehingga butuh beberapa menit.
- Tunggu ~1-2 menit agar HDFS & Hive Metastore benar-benar siap
  sebelum menjalankan pipeline.
- Cek kesiapan dengan: bash scripts/00_check_services.sh
------------------------------------------------------------------
EOF
