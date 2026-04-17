#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"
# This starts the full local stack: HDFS, PostgreSQL metastore, Hive metastore, and HiveServer2.
docker compose -f environment/docker-compose.hadoop-hive.yml up -d

cat <<'EOF'
Docker stack requested.

Useful checks:
- docker ps
- docker logs bank-namenode
- docker logs bank-hive-server
EOF
