# =====================================================================
# 01_build_up.ps1
# Membangun image Spark kustom dan menjalankan seluruh service Docker.
# Jalankan dari PowerShell pada Windows 11.
#   PS> .\scripts\01_build_up.ps1
# =====================================================================
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host ">>> [1/3] Membangun image Spark kustom (apple-spark:3.3.2)..." -ForegroundColor Cyan
docker compose build

Write-Host ">>> [2/3] Menjalankan seluruh service (mode detached)..." -ForegroundColor Cyan
docker compose up -d

Write-Host ">>> [3/3] Status container:" -ForegroundColor Cyan
docker compose ps

Write-Host @"

------------------------------------------------------------------
Catatan:
- Pertama kali dijalankan, image base (Hadoop/Hive/Spark) akan
  diunduh sehingga butuh beberapa menit.
- Tunggu ~1-2 menit agar HDFS & Hive Metastore benar-benar siap
  sebelum menjalankan pipeline.
- Cek kesiapan dengan: .\scripts\00_check_services.ps1
------------------------------------------------------------------
"@
