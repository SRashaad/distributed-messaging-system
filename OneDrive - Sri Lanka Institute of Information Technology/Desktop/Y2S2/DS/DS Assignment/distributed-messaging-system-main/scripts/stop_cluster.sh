#!/bin/bash
# =============================================================================
# Script: stop_cluster.sh
# Purpose: Stop all running node processes launched by start_cluster.sh.
#
# Usage:
#   chmod +x scripts/stop_cluster.sh
#   ./scripts/stop_cluster.sh
# =============================================================================

set -e

echo "Stopping all node processes..."

# TODO: Kill all processes matching the server binary
# pkill -f "cmd/server/main.go" 2>/dev/null && echo "Nodes stopped." || echo "No running nodes found."

echo "Done."
