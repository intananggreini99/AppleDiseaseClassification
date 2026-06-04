#!/usr/bin/env bash
# =====================================================================
# 00_check_services.sh
# Mengecek kesiapan seluruh service dan menampilkan URL Web UI.
# =====================================================================
set -uo pipefail
cd "$(dirname "$0")/.."

check() {
  # $1 = nama service, $2 = URL
  local name="$1" url="$2"
  if curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$url" | grep -qE "^(200|302|401)$"; then
    printf "  [ OK ]  %-18s %s\n" "$name" "$url"
  else
    printf "  [WAIT]  %-18s %s  (belum siap)\n" "$name" "$url"
  fi
}

echo "==================================================================="
echo " Status Container"
echo "==================================================================="
docker compose ps

echo
echo "==================================================================="
echo " Cek Web UI (HTTP)"
echo "==================================================================="
check "NameNode (HDFS)"   "http://localhost:9870"
check "DataNode (HDFS)"   "http://localhost:9864"
check "Spark Master"      "http://localhost:8080"
check "Spark Worker"      "http://localhost:8081"
check "Spark History"     "http://localhost:18080"
check "HiveServer2"       "http://localhost:10002"

echo
echo "==================================================================="
echo " Cek PostgreSQL (TCP 5432)"
echo "==================================================================="
if docker exec postgres pg_isready -U appleuser -d appledb >/dev/null 2>&1; then
  echo "  [ OK ]  PostgreSQL siap menerima koneksi (localhost:5432, db=appledb)"
else
  echo "  [WAIT]  PostgreSQL belum siap"
fi

cat <<'EOF'

-------------------------------------------------------------------
Daftar lengkap Web UI yang dapat dibuka di browser:
  NameNode (HDFS)      : http://localhost:9870
  DataNode (HDFS)      : http://localhost:9864
  Spark Master         : http://localhost:8080
  Spark Worker         : http://localhost:8081
  Spark History Server : http://localhost:18080
  HiveServer2 UI       : http://localhost:10002
  Spark Application UI : http://localhost:4040   (hanya saat job berjalan)
PostgreSQL (bukan web) : localhost:5432  (user=appleuser, db=appledb)
-------------------------------------------------------------------
EOF
