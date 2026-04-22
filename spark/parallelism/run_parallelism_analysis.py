from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import psutil
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from spark.common.bank_helpers import (
    CATEGORICAL_COLUMNS,
    NUMERIC_COLUMNS,
    OUTPUTS_PATH,
    age_band_expr,
    build_spark_session,
    clip_numeric_outliers,
    ensure_dir,
    load_bank_dataframe,
    save_dataframe,
)


def capture_resource_snapshot(label: str) -> dict[str, float]:
    process = psutil.Process(os.getpid())
    return {
        "label": label,
        "cpu_percent": psutil.cpu_percent(interval=1),
        "memory_mb": round(process.memory_info().rss / (1024 * 1024), 2),
        "timestamp": time.time(),
    }


def main() -> None:
    available_cores = os.cpu_count() or 4
    target_partitions = max(4, min(available_cores * 2, 16))

    spark = build_spark_session("banking-data-parallelism", shuffle_partitions=target_partitions)
    spark.sparkContext.setLogLevel("WARN")

    reports_dir = ensure_dir(OUTPUTS_PATH / "reports" / "parallelism")

    resource_log = [capture_resource_snapshot("before_load")]

    raw_df = load_bank_dataframe(spark)
    raw_df.show(5, truncate=False)

    # Repartitioning by job keeps one busy category from doing too much work on a single task.
    partitioned_df = raw_df.repartition(target_partitions, "job").cache()
    partitioned_df.count()
    resource_log.append(capture_resource_snapshot("after_repartition"))

    avg_balance_by_job = (
        partitioned_df.groupBy("job")
        .agg(F.avg("balance").alias("average_balance"))
        .orderBy(F.desc("average_balance"))
    )
    save_dataframe(avg_balance_by_job, reports_dir / "avg_balance_by_job.csv")

    # The dataset has no numeric loan amount, so balance is used as the closest monetary proxy.
    loan_proxy_age_groups = (
        partitioned_df.withColumn("age_band", age_band_expr("age"))
        .filter(F.col("loan") == "yes")
        .groupBy("age_band")
        .agg(
            F.count("*").alias("loan_client_count"),
            F.avg("balance").alias("average_balance_for_loan_clients"),
        )
        .orderBy(F.desc("average_balance_for_loan_clients"))
    )
    save_dataframe(loan_proxy_age_groups.limit(5), reports_dir / "loan_proxy_age_groups.csv")

    prepared_df = clip_numeric_outliers(partitioned_df, NUMERIC_COLUMNS).withColumn(
        "label",
        F.when(F.col("y") == "yes", 1.0).otherwise(0.0),
    )
    train_df, test_df = prepared_df.randomSplit([0.8, 0.2], seed=42)

    indexers = [
        StringIndexer(inputCol=column_name, outputCol=f"{column_name}_idx", handleInvalid="keep")
        for column_name in CATEGORICAL_COLUMNS
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{column_name}_idx" for column_name in CATEGORICAL_COLUMNS],
        outputCols=[f"{column_name}_ohe" for column_name in CATEGORICAL_COLUMNS],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=NUMERIC_COLUMNS + [f"{column_name}_ohe" for column_name in CATEGORICAL_COLUMNS],
        outputCol="features",
        handleInvalid="keep",
    )
    classifier = LogisticRegression(labelCol="label", featuresCol="features", maxIter=80)

    pipeline = Pipeline(stages=indexers + [encoder, assembler, classifier])

    # The job description makes the Spark UI easier to read during the demo.
    spark.sparkContext.setJobDescription("Parallel model training on partitioned data")
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)
    evaluation_df = predictions.groupBy("label", "prediction").count().orderBy("label", "prediction")
    save_dataframe(evaluation_df, reports_dir / "prediction_confusion_matrix_counts.csv")
    resource_log.append(capture_resource_snapshot("after_model_training"))

    task_notes = {
        "available_cores": available_cores,
        "target_partitions": target_partitions,
        "partition_strategy": "repartition by job to spread category-heavy work more evenly across tasks",
        "task_management_note": "Cached intermediate data and reused partitioned DataFrames to avoid repeated scans during aggregation and model training.",
    }

    (reports_dir / "resource_log.json").write_text(json.dumps(resource_log, indent=2), encoding="utf-8")
    (reports_dir / "parallelism_summary.json").write_text(json.dumps(task_notes, indent=2), encoding="utf-8")

    print("Parallelism outputs saved to", reports_dir)
    spark.stop()


if __name__ == "__main__":
    main()
