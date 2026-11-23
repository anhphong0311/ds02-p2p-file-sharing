# Demo trên 1 máy Windows: mỗi node là 1 cửa sổ PowerShell riêng (tiến trình riêng).
#   powershell -ExecutionPolicy Bypass -File scripts\demo_local.ps1                 # 1 tracker, không TLS
#   powershell -ExecutionPolicy Bypass -File scripts\demo_local.ps1 -TwoTrackers -Tls   # đầy đủ tính năng
# Tham số:
#   -SizeMB 30      kích thước file mẫu
#   -Delay 0.3      giây/chunk mỗi seeder (giả lập băng thông, để kịp thao tác khi demo)
#   -TwoTrackers    chạy 2 tracker nhân bản (cổng 8000 và 8001)
#   -Tls            bật TLS hai chiều (tự tạo chứng chỉ trong certs\ nếu chưa có)
#   -KeepData       giữ dữ liệu cũ (mặc định xóa data\, logs\, downloads\ để demo từ đầu)
# Chèn lỗi trong lúc demo: scripts\fault_inject.ps1
param(
    [int]$SizeMB = 30,
    [double]$Delay = 0.3,
    [switch]$TwoTrackers,
    [switch]$Tls,
    [switch]$KeepData
)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$sample = "samples\demo.bin"

if (-not $KeepData) {
    foreach ($d in "data", "logs", "downloads") { if (Test-Path $d) { Remove-Item -Recurse -Force $d } }
}
if (-not (Test-Path $sample)) {
    python scripts\gen_file.py $sample --mb $SizeMB --seed 42
}

$scheme = "http"
if ($Tls) {
    $scheme = "https"
    if (-not (Test-Path "certs\node.crt")) { python scripts\gen_certs.py --out certs }
}
$trackers = @("${scheme}://127.0.0.1:8000")
if ($TwoTrackers) { $trackers += "${scheme}://127.0.0.1:8001" }
$trackerUrl = $trackers -join ","

# Biến môi trường dùng chung cho mọi cửa sổ (fault_inject.ps1 cũng đọc file này)
$tlsValue = if ($Tls) { "1" } else { "0" }
@"
`$env:TRACKER_URL = '$trackerUrl'
`$env:TLS_ENABLED = '$tlsValue'
`$env:PYTHONUNBUFFERED = '1'
"@ | Set-Content -Encoding utf8 ".demo_env.ps1"

function Start-Node($title, $cmd) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command",
        "`$host.UI.RawUI.WindowTitle='$title'; Set-Location '$root'; . .\.demo_env.ps1; $cmd"
}

Start-Node "TRACKER (8000)" "python -m src.tracker.server --port 8000 --node-id tracker"
if ($TwoTrackers) {
    Start-Node "TRACKER2 (8001)" "python -m src.tracker.server --port 8001 --node-id tracker2"
}
Start-Sleep -Seconds 2
Start-Node "SEEDER peerA" "python -m src.peer.main --peer-id peerA --port 7001 --upload-delay $Delay seed $sample"
Start-Node "SEEDER peerB" "python -m src.peer.main --peer-id peerB --port 7002 --upload-delay $Delay seed $sample"
Start-Sleep -Seconds 3
Start-Node "LEECHER peerC" "python -m src.peer.main --peer-id peerC --port 7003 download demo.bin --out downloads\peerC"

Write-Host "Trackers: $trackerUrl   TLS: $Tls"
Write-Host "Log: thu muc logs\   Kiem tra hash: certutil -hashfile downloads\peerC\demo.bin SHA256"
Write-Host "Chen loi: powershell -File scripts\fault_inject.ps1 <kill-peerA | bad-seeder | late-leecher | kill-tracker | start-tracker | restart-peerC>"
