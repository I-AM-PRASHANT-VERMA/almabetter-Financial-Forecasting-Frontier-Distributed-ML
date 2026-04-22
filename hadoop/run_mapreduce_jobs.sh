#!/usr/bin/env bash
set -euo pipefail

MAPREDUCE_DIR="/workspace/mapreduce"
HADOOP_BIN="/opt/hadoop-3.2.1/bin"
STREAMING_JAR="/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"

# The raw file is refreshed each time so both Hadoop and Hive always start from the same place.
docker exec bank-namenode ${HADOOP_BIN}/hdfs dfs -mkdir -p /project/banking/raw
docker exec bank-namenode ${HADOOP_BIN}/hdfs dfs -put -f /project_data/bank.csv /project/banking/raw/bank.csv
docker exec bank-namenode ${HADOOP_BIN}/hdfs dfs -mkdir -p /project/banking/output

run_streaming_job() {
  local job_name="$1"
  local mapper_file="$2"
  local reducer_file="$3"

  # Each output folder is cleared first so reruns stay predictable during submission checks.
  docker exec bank-namenode ${HADOOP_BIN}/hdfs dfs -rm -r -f "/project/banking/output/${job_name}" || true
  docker exec bank-namenode bash -lc "${HADOOP_BIN}/hadoop jar $STREAMING_JAR \
    -files $MAPREDUCE_DIR/${mapper_file},$MAPREDUCE_DIR/${reducer_file} \
    -mapper 'python3 ${mapper_file}' \
    -reducer 'python3 ${reducer_file}' \
    -input /project/banking/raw/bank.csv \
    -output /project/banking/output/${job_name}"
}

run_streaming_job "avg_balance_by_job" "avg_balance_by_job_mapper.py" "avg_balance_by_job_reducer.py"
run_streaming_job "housing_loan_by_education" "housing_loan_by_education_mapper.py" "housing_loan_by_education_reducer.py"
run_streaming_job "contacts_by_month_subscription" "contacts_by_month_subscription_mapper.py" "contacts_by_month_subscription_reducer.py"
run_streaming_job "avg_duration_by_poutcome" "avg_duration_by_poutcome_mapper.py" "avg_duration_by_poutcome_reducer.py"
run_streaming_job "age_balance_summary" "age_balance_summary_mapper.py" "age_balance_summary_reducer.py"
