sn#!/bin/bash
# =============================================================================
# Script: start_cluster.sh
# Purpose: Launch a local 3-node cluster for development and testing.
#          Each node runs as a separate background process with its own
#          port and peer configuration.
#
# Usage:
#   chmod +x scripts/start_cluster.sh
#   ./scripts/start_cluster.sh
#
# To stop all nodes:
#   ./scripts/stop_cluster.sh
# =============================================================================

set -e

echo "Starting 3-node cluster..."

# Start Node 1 on port 5001 with peers on 5002 and 5003
go run cmd/server/main.go --id node1 --port 5001 --peers "localhost:5002,localhost:5003" &

# Start Node 2 on port 5002 with peers on 5001 and 5003
go run cmd/server/main.go --id node2 --port 5002 --peers "localhost:5001,localhost:5003" &

# Start Node 3 on port 5003 with peers on 5001 and 5002
go run cmd/server/main.go --id node3 --port 5003 --peers "localhost:5001,localhost:5002" &

echo "Cluster started. Press Ctrl+C to stop."

# Wait for all background processes
wait
