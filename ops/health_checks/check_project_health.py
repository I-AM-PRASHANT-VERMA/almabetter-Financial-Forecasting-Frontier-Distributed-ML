from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "reports" / "health"
REPORT_PATH = OUTPUT_DIR / "project_health_report.json"


def run_command(command: list[str]) -> tuple[bool, str]:
    # Capturing stdout and stderr together keeps the final report easier to read.
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=PROJECT_ROOT,
        check=False,
    )
    return completed.returncode == 0, completed.stdout.strip()


def build_check(name: str, passed: bool, details: str) -> dict[str, str | bool]:
    return {
        "name": name,
        "passed": passed,
        "details": details,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    checks: list[dict[str, str | bool]] = []

    # The Docker services are the first dependency because Hadoop and Hive both sit on top of them.
    ok, docker_status = run_command(
        [
            "docker",
            "ps",
            "--format",
            "{{.Names}}|{{.Status}}",
        ]
    )
    checks.append(build_check("docker_services", ok, docker_status))

    # A small HDFS read is enough to prove the Hadoop output path is still available.
    ok, hdfs_output = run_command(
        [
            "docker",
            "exec",
            "bank-namenode",
            "/opt/hadoop-3.2.1/bin/hdfs",
            "dfs",
            "-cat",
            "/project/banking/output/avg_balance_by_job/part-00000",
        ]
    )
    checks.append(build_check("hadoop_output", ok and "retired" in hdfs_output, hdfs_output))

    # This count query is a quick truth check for the Hive table and metastore.
    ok, hive_output = run_command(
        [
            "docker",
            "exec",
            "bank-hive-server",
            "beeline",
            "-u",
            "jdbc:hive2://localhost:10000/default",
            "-n",
            "hive",
            "-e",
            "USE banking_data; SELECT COUNT(*) AS total_clients FROM client_info;",
        ]
    )
    checks.append(build_check("hive_query", ok and "4521" in hive_output, hive_output))

    # These generated files are the fastest proof that the Spark parts ran successfully.
    eda_summary = PROJECT_ROOT / "outputs" / "reports" / "spark_eda" / "eda_summary.json"
    ml_metrics = PROJECT_ROOT / "outputs" / "reports" / "spark_ml" / "model_metrics.json"
    parallel_summary = PROJECT_ROOT / "outputs" / "reports" / "parallelism" / "parallelism_summary.json"
    model_path = PROJECT_ROOT / "outputs" / "models" / "spark_ml" / "best_logistic_pipeline"

    checks.append(build_check("spark_eda_report", eda_summary.exists(), str(eda_summary)))
    checks.append(build_check("spark_ml_metrics", ml_metrics.exists(), str(ml_metrics)))
    checks.append(build_check("parallelism_report", parallel_summary.exists(), str(parallel_summary)))
    checks.append(build_check("spark_model_folder", model_path.exists(), str(model_path)))

    overall_pass = all(bool(item["passed"]) for item in checks)
    payload = {
        "overall_pass": overall_pass,
        "checks": checks,
    }
    REPORT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # The console output is meant to be readable even without opening the JSON file.
    for item in checks:
        status = "PASS" if item["passed"] else "FAIL"
        print("[{0}] {1}".format(status, item["name"]))

    print("Health report saved to {0}".format(REPORT_PATH))
    return 0 if overall_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
