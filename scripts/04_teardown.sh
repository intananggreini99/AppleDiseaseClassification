#!/usr/bin/env bash
# =====================================================================
# 04_teardown.sh
# Menghentikan dan menghapus seluruh container.
#
#   tanpa argumen : hentikan container, data (volume) TETAP tersimpan.
#   --purge       : hentikan + HAPUS volume (data HDFS/PostgreSQL hilang).
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${1:-}" = "--purge" ]; then
  echo ">>> Menghentikan container dan MENGHAPUS seluruh volume/data..."
  docker compose down -v
  echo ">>> Selesai. Semua data HDFS & PostgreSQL telah dihapus."
else
  echo ">>> Menghentikan container (volume/data dipertahankan)..."
  docker compose down
  echo ">>> Selesai. Jalankan ulang dengan: bash scripts/01_build_up.sh"
  echo ">>> Untuk menghapus data juga, jalankan: bash scripts/04_teardown.sh --purge"
fi
