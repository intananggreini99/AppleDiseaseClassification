# =====================================================================
# 00_check_services.ps1
# Mengecek kesiapan seluruh service dan menampilkan URL Web UI.
#   PS> .\scripts\00_check_services.ps1
# =====================================================================
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..")

function Check-Url($name, $url) {
    try {
        $resp = Invoke-WebRequest -Uri $url -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
        $code = $resp.StatusCode
    } catch {
        $code = $_.Exception.Response.StatusCode.value__
    }
    if ($code -in 200, 302, 401) {
        Write-Host ("  [ OK ]  {0,-18} {1}" -f $name, $url) -ForegroundColor Green
    } else {
        Write-Host ("  [WAIT]  {0,-18} {1}  (belum siap)" -f $name, $url) -ForegroundColor Yellow
    }
}

Write-Host "==================================================================="
Write-Host " Status Container"
Write-Host "==================================================================="
docker compose ps

Write-Host ""
Write-Host "==================================================================="
Write-Host " Cek Web UI (HTTP)"
Write-Host "==================================================================="
Check-Url "NameNode (HDFS)" "http://localhost:9870"
Check-Url "DataNode (HDFS)" "http://localhost:9864"
Check-Url "Spark Master"    "http://localhost:8080"
Check-Url "Spark Worker"    "http://localhost:8081"
Check-Url "Spark History"   "http://localhost:18080"
Check-Url "HiveServer2"     "http://localhost:10002"

Write-Host ""
Write-Host "==================================================================="
Write-Host " Cek PostgreSQL (TCP 5432)"
Write-Host "==================================================================="
docker exec postgres pg_isready -U appleuser -d appledb 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [ OK ]  PostgreSQL siap (localhost:5432, db=appledb)" -ForegroundColor Green
} else {
    Write-Host "  [WAIT]  PostgreSQL belum siap" -ForegroundColor Yellow
}

Write-Host @"

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
"@
