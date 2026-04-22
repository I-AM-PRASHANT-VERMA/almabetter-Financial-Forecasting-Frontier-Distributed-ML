#!/usr/bin/env bash
set -euo pipefail

# Beeline runs against the custom HiveServer2 container so the whole query file can be replayed in one shot.
docker exec bank-hive-server beeline -u jdbc:hive2://localhost:10000/default -n hive -f /workspace/hive/queries/banking_analysis.hql
