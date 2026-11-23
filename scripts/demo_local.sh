#!/usr/bin/env bash
# Demo trên 1 máy Linux/macOS/Git Bash: tracker + 2 seeder + 1 leecher chạy nền, log trong logs/.
# Dùng:  bash scripts/demo_local.sh [SIZE_MB] [DELAY]
#        bash scripts/demo_local.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${1:-}" == "stop" ]]; then
  [[ -f .demo_pids ]] && xargs kill < .demo_pids 2>/dev/null || true
  rm -f .demo_pids
  exit 0
fi

SIZE_MB=${1:-30}
DELAY=${2:-0.05}
SAMPLE=samples/demo.bin
mkdir -p logs
[[ -f $SAMPLE ]] || python scripts/gen_file.py "$SAMPLE" --mb "$SIZE_MB" --seed 42

python -m src.tracker.server --port 8000 > logs/tracker.out 2>&1 & echo $! > .demo_pids
sleep 2
python -m src.peer.main --peer-id peerA --port 7001 --upload-delay "$DELAY" seed "$SAMPLE" > logs/peerA.out 2>&1 & echo $! >> .demo_pids
python -m src.peer.main --peer-id peerB --port 7002 --upload-delay "$DELAY" seed "$SAMPLE" > logs/peerB.out 2>&1 & echo $! >> .demo_pids
sleep 3
python -m src.peer.main --peer-id peerC --port 7003 download demo.bin --out downloads/peerC > logs/peerC.out 2>&1 & echo $! >> .demo_pids

echo "Started. Follow: tail -f logs/peerC.log    Stop: bash scripts/demo_local.sh stop"
echo "Kill a source mid-download:  kill \$(sed -n 2p .demo_pids)"
