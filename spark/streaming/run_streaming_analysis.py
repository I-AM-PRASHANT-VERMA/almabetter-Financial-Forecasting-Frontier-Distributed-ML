from __future__ import annotations

import time
import sys
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from spark.common.bank_helpers import (
    CATEGORICAL_COLUMNS,
    NUMERIC_COLUMNS,
    STREAMING_SCHEMA,
    build_spark_session,
    clip_numeric_outliers,
    load_bank_dataframe,
)


def build_prediction_pipeline():
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
    return Pipeline(stages=indexers + [encoder, assembler, classifier])


def main(runtime_seconds: int = 45) -> None:
    spark = build_spark_session("banking-structured-streaming", shuffle_partitions=8)
    spark.sparkContext.setLogLevel("WARN")

    input_dir = PROJECT_ROOT / "data" / "stream_chunks" / "input"

    historical_df = load_bank_dataframe(spark)
    # The model is trained on the historical file first, then reused on the incoming stream.
    historical_df = clip_numeric_outliers(historical_df, NUMERIC_COLUMNS).withColumn(
        "label",
        F.when(F.col("y") == "yes", 1.0).otherwise(0.0),
    )

    pipeline_model = build_prediction_pipeline().fit(historical_df)

    streaming_df = (
        spark.readStream.option("header", True)
        .schema(STREAMING_SCHEMA)
        .csv(str(input_dir))
    )

    streaming_base = streaming_df.withColumn("label", F.when(F.col("y") == "yes", 1.0).otherwise(0.0))
    predicted_stream = pipeline_model.transform(streaming_base)

    # This view mirrors the kind of live KPI table a team might monitor during a campaign.
    realtime_job_metrics = (
        streaming_base.groupBy("job")
        .agg(
            F.avg("balance").alias("avg_balance"),
            F.avg("duration").alias("avg_duration"),
            F.count("*").alias("transaction_count"),
        )
    )

    window_10_seconds = (
        streaming_base.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "10 seconds"))
        .agg(
            F.count("*").alias("transaction_count"),
            F.avg("balance").alias("avg_balance"),
        )
        .select("window", "transaction_count", "avg_balance")
    )

    window_1_minute = (
        streaming_base.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute"))
        .agg(
            F.count("*").alias("transaction_count"),
            F.avg("balance").alias("avg_balance"),
        )
        .select("window", "transaction_count", "avg_balance")
    )

    prediction_output = predicted_stream.select(
        "event_time",
        "age",
        "job",
        "balance",
        "duration",
        "y",
        "prediction",
        vector_to_array("probability")[1].alias("subscription_probability"),
    )

    # Console output is enough for the assignment because it shows the stream progressing batch by batch.
    queries = [
        realtime_job_metrics.writeStream.outputMode("complete").format("console").option("truncate", False).option("numRows", 20).queryName("realtime_job_metrics").start(),
        window_10_seconds.writeStream.outputMode("append").format("console").option("truncate", False).option("numRows", 20).queryName("window_10_seconds").start(),
        window_1_minute.writeStream.outputMode("append").format("console").option("truncate", False).option("numRows", 20).queryName("window_1_minute").start(),
        prediction_output.writeStream.outputMode("append").format("console").option("truncate", False).option("numRows", 20).queryName("prediction_output").start(),
    ]

    time.sleep(runtime_seconds)

    for query in queries:
        if query.isActive:
            query.stop()

    spark.stop()


if __name__ == "__main__":
    main()
