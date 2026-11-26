# Khởi động dashboard web local cho demo DS02.
param([int]$Port = 8088)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "DS02 Web Demo: http://127.0.0.1:$Port"
Write-Host "Giữ cửa sổ này mở. Nhấn Ctrl+C để dừng dashboard."
Start-Process "http://127.0.0.1:$Port"
python -m src.web_demo.server --port $Port
