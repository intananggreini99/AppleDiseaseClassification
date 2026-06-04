# =====================================================================
# 04_teardown.ps1
# Menghentikan dan menghapus seluruh container.
#
#   PS> .\scripts\04_teardown.ps1           # data (volume) tetap tersimpan
#   PS> .\scripts\04_teardown.ps1 -Purge    # hapus volume (data hilang)
# =====================================================================
param(
    [switch]$Purge
)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if ($Purge) {
    Write-Host ">>> Menghentikan container dan MENGHAPUS seluruh volume/data..." -ForegroundColor Yellow
    docker compose down -v
    Write-Host ">>> Selesai. Semua data HDFS & PostgreSQL telah dihapus."
} else {
    Write-Host ">>> Menghentikan container (volume/data dipertahankan)..." -ForegroundColor Cyan
    docker compose down
    Write-Host ">>> Selesai. Jalankan ulang dengan: .\scripts\01_build_up.ps1"
    Write-Host ">>> Untuk menghapus data juga: .\scripts\04_teardown.ps1 -Purge"
}
