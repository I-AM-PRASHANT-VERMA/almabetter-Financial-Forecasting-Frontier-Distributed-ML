from __future__ import annotations

from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "bank.csv"
OUTPUTS_PATH = PROJECT_ROOT / "outputs"

MONTH_ORDER = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

BANK_SCHEMA = StructType(
    [
        StructField("age", IntegerType(), True),
        StructField("job", StringType(), True),
        StructField("marital", StringType(), True),
        StructField("education", StringType(), True),
        StructField("default", StringType(), True),
        StructField("balance", IntegerType(), True),
        StructField("housing", StringType(), True),
        StructField("loan", StringType(), True),
        StructField("contact", StringType(), True),
        StructField("day", IntegerType(), True),
        StructField("month", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("campaign", IntegerType(), True),
        StructField("pdays", IntegerType(), True),
        StructField("previous", IntegerType(), True),
        StructField("poutcome", StringType(), True),
        StructField("y", StringType(), True),
    ]
)

STREAMING_SCHEMA = StructType(BANK_SCHEMA.fields + [StructField("event_time", TimestampType(), True)])

NUMERIC_COLUMNS = ["age", "balance", "day", "duration", "campaign", "pdays", "previous"]
CATEGORICAL_COLUMNS = ["job", "marital", "education", "default", "housing", "loan", "contact", "month", "poutcome"]


def build_spark_session(app_name: str, shuffle_partitions: int = 8) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # local[*] is enough here because the assignment is about the workflow, not cluster size.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .getOrCreate()
    )


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_bank_dataframe(spark: SparkSession, input_path: Path | str = RAW_DATA_PATH) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(BANK_SCHEMA)
        .csv(str(input_path))
    )


def month_order_expr(column_name: str = "month") -> Column:
    mapping_items = []
    for month_name, order_value in MONTH_ORDER.items():
        mapping_items.extend([F.lit(month_name), F.lit(order_value)])
    # The month labels are text, so a manual map keeps the ordering stable in reports.
    month_map = F.create_map(*mapping_items)
    return F.coalesce(month_map[F.col(column_name)], F.lit(99))


def quarter_expr(column_name: str = "month") -> Column:
    month_num = month_order_expr(column_name)
    return (
        F.when(month_num.isin(1, 2, 3), F.lit("Q1"))
        .when(month_num.isin(4, 5, 6), F.lit("Q2"))
        .when(month_num.isin(7, 8, 9), F.lit("Q3"))
        .otherwise(F.lit("Q4"))
    )


def age_group_expr(column_name: str = "age") -> Column:
    return (
        F.when(F.col(column_name) < 30, F.lit("<30"))
        .when((F.col(column_name) >= 30) & (F.col(column_name) <= 60), F.lit("30-60"))
        .otherwise(F.lit(">60"))
    )


def age_band_expr(column_name: str = "age") -> Column:
    return (
        F.when(F.col(column_name) < 30, F.lit("18-29"))
        .when((F.col(column_name) >= 30) & (F.col(column_name) < 40), F.lit("30-39"))
        .when((F.col(column_name) >= 40) & (F.col(column_name) < 50), F.lit("40-49"))
        .when((F.col(column_name) >= 50) & (F.col(column_name) < 60), F.lit("50-59"))
        .otherwise(F.lit("60+"))
    )


def clip_numeric_outliers(df: DataFrame, columns: Iterable[str], lower_q: float = 0.01, upper_q: float = 0.99) -> DataFrame:
    clipped_df = df
    for column_name in columns:
        lower_bound, upper_bound = df.approxQuantile(column_name, [lower_q, upper_q], 0.01)
        # This trims extreme values without dropping rows, which keeps counts consistent across parts.
        clipped_df = clipped_df.withColumn(
            column_name,
            F.when(F.col(column_name) < lower_bound, F.lit(lower_bound))
            .when(F.col(column_name) > upper_bound, F.lit(upper_bound))
            .otherwise(F.col(column_name)),
        )
    return clipped_df


def save_dataframe(df: DataFrame, output_path: Path, mode: str = "overwrite") -> None:
    ensure_dir(output_path.parent)
    pandas_df = df.toPandas()
    pandas_df.to_csv(output_path, index=False)


def extract_feature_names(metadata: dict) -> list[str]:
    attrs = metadata.get("ml_attr", {}).get("attrs", {})
    ordered_features: list[tuple[int, str]] = []
    for attr_group in attrs.values():
        for item in attr_group:
            ordered_features.append((item["idx"], item["name"]))
    return [name for _, name in sorted(ordered_features, key=lambda item: item[0])]
