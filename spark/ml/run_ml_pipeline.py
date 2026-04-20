from __future__ import annotations

import json
import sys
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from spark.common.bank_helpers import (
    CATEGORICAL_COLUMNS,
    NUMERIC_COLUMNS,
    OUTPUTS_PATH,
    build_spark_session,
    clip_numeric_outliers,
    ensure_dir,
    extract_feature_names,
    load_bank_dataframe,
    save_dataframe,
)


def evaluate_predictions(predictions, label_col: str = "label") -> dict[str, float]:
    metrics = {
        "accuracy": MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="accuracy").evaluate(predictions),
        "f1": MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="f1").evaluate(predictions),
        "weighted_precision": MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions),
        "weighted_recall": MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="weightedRecall").evaluate(predictions),
        "area_under_roc": BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="rawPrediction", metricName="areaUnderROC").evaluate(predictions),
        "area_under_pr": BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="rawPrediction", metricName="areaUnderPR").evaluate(predictions),
    }
    return {key: float(value) for key, value in metrics.items()}


def build_feature_pipeline(model_type: str):
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

    if model_type == "logistic":
        estimator = LogisticRegression(labelCol="label", featuresCol="features", maxIter=100)
    elif model_type == "tree":
        estimator = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=6, minInstancesPerNode=10)
    else:
        raise ValueError(f"Unsupported model type: {model_type}")

    return Pipeline(stages=indexers + [encoder, assembler, estimator])


def main() -> None:
    spark = build_spark_session("banking-spark-ml", shuffle_partitions=8)

    reports_dir = ensure_dir(OUTPUTS_PATH / "reports" / "spark_ml")
    models_dir = ensure_dir(OUTPUTS_PATH / "models" / "spark_ml")

    base_df = load_bank_dataframe(spark)

    # The source file has no nulls, but this keeps preprocessing defensive for reruns with edited data.
    cleaned_df = clip_numeric_outliers(base_df, NUMERIC_COLUMNS)
    prepared_df = cleaned_df.withColumn("label", F.when(F.col("y") == "yes", 1.0).otherwise(0.0))

    train_df, test_df = prepared_df.randomSplit([0.8, 0.2], seed=42)
    train_df = train_df.cache()
    test_df = test_df.cache()

    logistic_pipeline = build_feature_pipeline("logistic")
    tree_pipeline = build_feature_pipeline("tree")

    logistic_model = logistic_pipeline.fit(train_df)
    tree_model = tree_pipeline.fit(train_df)

    logistic_predictions = logistic_model.transform(test_df)
    tree_predictions = tree_model.transform(test_df)

    # Two models are compared so the report can justify the final choice instead of showing one blind attempt.
    logistic_metrics = evaluate_predictions(logistic_predictions)
    tree_metrics = evaluate_predictions(tree_predictions)

    tuning_pipeline = build_feature_pipeline("logistic")
    tuning_estimator = tuning_pipeline.getStages()[-1]
    param_grid = (
        ParamGridBuilder()
        .addGrid(tuning_estimator.regParam, [0.0, 0.05, 0.1])
        .addGrid(tuning_estimator.elasticNetParam, [0.0, 0.5, 1.0])
        .build()
    )

    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    cross_validator = CrossValidator(
        estimator=tuning_pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=2,
        seed=42,
    )
    cv_model = cross_validator.fit(train_df)
    best_model = cv_model.bestModel
    tuned_predictions = best_model.transform(test_df)
    tuned_metrics = evaluate_predictions(tuned_predictions)

    # Feature names are pulled from pipeline metadata so the importance table stays readable in the report.
    transformed_sample = best_model.transform(prepared_df.limit(200))
    feature_names = extract_feature_names(transformed_sample.schema["features"].metadata)

    tree_stage = tree_model.stages[-1]
    tree_feature_importances = tree_stage.featureImportances.toArray().tolist()
    feature_importance_rows = sorted(
        zip(feature_names, tree_feature_importances),
        key=lambda item: item[1],
        reverse=True,
    )

    feature_importance_df = spark.createDataFrame(feature_importance_rows[:20], ["feature_name", "importance"])
    save_dataframe(feature_importance_df, reports_dir / "decision_tree_feature_importance.csv")

    prediction_sample = tuned_predictions.select("label", "prediction", "probability").limit(200)
    save_dataframe(prediction_sample, reports_dir / "prediction_sample.csv")

    metrics_payload = {
        "baseline_logistic_regression": logistic_metrics,
        "baseline_decision_tree": tree_metrics,
        "tuned_logistic_regression": tuned_metrics,
    }
    (reports_dir / "model_metrics.json").write_text(json.dumps(metrics_payload, indent=2), encoding="utf-8")

    best_model.write().overwrite().save(str(models_dir / "best_logistic_pipeline"))

    print("Spark ML metrics saved to", reports_dir)
    print("Best pipeline saved to", models_dir / "best_logistic_pipeline")
    spark.stop()


if __name__ == "__main__":
    main()
