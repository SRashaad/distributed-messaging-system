#!/usr/bin/env bash
# Start N nodes in the background; each writes to .cluster-logs/node*.log
# Default: 3 nodes on 5001–5003, ZooKeeper localhost:2181
#
# Usage:
#   chmod +x scripts/start-cluster.sh
#   ./scripts/start-cluster.sh
#   NODE_COUNT=5 ZK=localhost:2181 ./scripts/start-cluster.sh
#   USE_BINARY=./bin/server ./scripts/start-cluster.sh   # build first: go build -o bin/server ./cmd/server
#
# Tail output:  tail -f .cluster-logs/node1.log
# Stop all:    kill $(cat .cluster-logs/pids.txt)   # after we write pids

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NODE_COUNT="${NODE_COUNT:-3}"
ZK="${ZK:-localhost:2181}"
USE_BINARY="${USE_BINARY:-}"

peers_for_port() {
  local my="$1" i p
  local peers=()
  for ((i=1; i<=NODE_COUNT; i++)); do
    p=$((5000 + i))
    if [[ "$p" -ne "$my" ]]; then
      peers+=("localhost:$p")
    fi
  done
  (IFS=,; echo "${peers[*]}")
}

LOGDIR="$ROOT/.cluster-logs"
mkdir -p "$LOGDIR"
: >"$LOGDIR/pids.txt"

for ((i=1; i<=NODE_COUNT; i++)); do
  id="node${i}"
  port=$((5000 + i))
  peers="$(peers_for_port "$port")"
  log="$LOGDIR/node${i}.log"
  if [[ -n "$USE_BINARY" ]]; then
    (
      cd "$ROOT"
      exec "$USE_BINARY" -id "$id" -port "$port" -peers "$peers" -zk "$ZK"
    ) >"$log" 2>&1 &
  else
    (
      cd "$ROOT"
      exec go run ./cmd/server -id "$id" -port "$port" -peers "$peers" -zk "$ZK"
    ) >"$log" 2>&1 &
  fi
  echo $! >>"$LOGDIR/pids.txt"
  echo "Started $id on port $port (log: $log)"
done

echo ""
echo "All $NODE_COUNT nodes running. Tail: tail -f $LOGDIR/node1.log"
echo "Stop: kill \$(tr '\n' ' ' < \"$LOGDIR/pids.txt\")"
