#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv-wsl"
REQUIREMENTS_FILE="$PROJECT_ROOT/environment/requirements-wsl.txt"

# Java powers Spark, and the virtual environment keeps the Python side isolated from the laptop setup.
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk python3-venv python3-pip docker-compose-plugin

python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

# Install the pinned project packages inside the WSL-only environment.
python -m pip install --upgrade pip
python -m pip install -r "$REQUIREMENTS_FILE"

cat <<EOF
WSL setup finished.

Next steps:
1. source "$VENV_DIR/bin/activate"
2. python -c "import pyspark; print(pyspark.__version__)"
3. docker compose -f environment/docker-compose.hadoop-hive.yml up -d
EOF
