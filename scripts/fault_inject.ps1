# Các kịch bản chèn lỗi khi demo (chạy SAU scripts\demo_local.ps1, trong một cửa sổ khác):
#   powershell -File scripts\fault_inject.ps1 kill-peerA      # tắt đột ngột seeder peerA -> peerC chuyển nguồn
#   powershell -File scripts\fault_inject.ps1 bad-seeder      # thêm seeder gửi chunk hỏng
#   powershell -File scripts\fault_inject.ps1 late-leecher    # thêm peerD -> thấy peerC upload khi chưa tải xong
#   powershell -File scripts\fault_inject.ps1 kill-tracker    # tắt tracker chính (cổng 8000)
#   powershell -File scripts\fault_inject.ps1 start-tracker   # chạy lại tracker chính
#   powershell -File scripts\fault_inject.ps1 restart-peerC   # tắt peerC giữa chừng rồi chạy lại -> resume
param([Parameter(Mandatory = $true)][string]$Scenario)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if (-not (Test-Path ".demo_env.ps1")) { Write-Host "Hay chay scripts\demo_local.ps1 truoc"; exit 1 }

function Start-Node($title, $cmd) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command",
        "`$host.UI.RawUI.WindowTitle='$title'; Set-Location '$root'; . .\.demo_env.ps1; $cmd"
}

function Stop-Matching($pattern, $label) {
    $procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -match $pattern }
    if (-not $procs) { Write-Host "Khong tim thay $label"; return }
    foreach ($p in $procs) {
        Stop-Process -Id $p.ProcessId -Force
        Write-Host "Killed $label (pid $($p.ProcessId))"
        # Đóng luôn cửa sổ PowerShell chứa tiến trình đó để nhìn rõ node đã chết
        $parent = Get-Process -Id $p.ParentProcessId -ErrorAction SilentlyContinue
        if ($parent -and $parent.ProcessName -eq "powershell") { Stop-Process -Id $parent.Id -Force }
    }
}

switch ($Scenario) {
    "kill-peerA"    { Stop-Matching "--peer-id peerA" "peerA" }
    "bad-seeder"    { Start-Node "BAD SEEDER peerBad" "python -m src.peer.main --peer-id peerBad --port 7005 --corrupt-chunks all seed samples\demo.bin" }
    "late-leecher"  { Start-Node "LEECHER peerD" "python -m src.peer.main --peer-id peerD --port 7004 download demo.bin --out downloads\peerD" }
    "kill-tracker"  { Stop-Matching "src.tracker.server --port 8000" "tracker (8000)" }
    "start-tracker" { Start-Node "TRACKER (8000)" "python -m src.tracker.server --port 8000 --node-id tracker" }
    "restart-peerC" {
        Stop-Matching "--peer-id peerC" "peerC"
        Start-Sleep -Seconds 1
        Start-Node "LEECHER peerC (resume)" "python -m src.peer.main --peer-id peerC --port 7003 download demo.bin --out downloads\peerC"
    }
    default { Write-Host "Unknown scenario: $Scenario" }
}
