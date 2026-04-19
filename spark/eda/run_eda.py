from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from spark.common.bank_helpers import (
    OUTPUTS_PATH,
    age_group_expr,
    build_spark_session,
    ensure_dir,
    load_bank_dataframe,
    month_order_expr,
    quarter_expr,
    save_dataframe,
)


def main() -> None:
    spark = build_spark_session("banking-spark-eda", shuffle_partitions=8)

    reports_dir = ensure_dir(OUTPUTS_PATH / "reports" / "spark_eda")
    figures_dir = ensure_dir(OUTPUTS_PATH / "figures" / "spark_eda")

    df = load_bank_dataframe(spark)

    # This keeps the raw inspection output easy to compare with the original CSV.
    print("Schema")
    df.printSchema()

    print("First five rows")
    df.show(5, truncate=False)

    numeric_summary = df.select("age", "balance", "duration", "campaign", "pdays", "previous").summary()
    save_dataframe(numeric_summary, reports_dir / "numeric_summary.csv")

    high_balance_df = df.filter(F.col("balance") > 1000)
    save_dataframe(high_balance_df.limit(200), reports_dir / "high_balance_sample.csv")

    df_with_quarter = (
        df.withColumn("quarter", quarter_expr("month"))
        .withColumn("age_group", age_group_expr("age"))
        .withColumn("job_marital", F.concat_ws("_", F.col("job"), F.col("marital")))
        .withColumn("contact_upper", F.upper(F.col("contact")))
    )

    # A UDF is included here because the assignment expects one custom transformation example.
    age_group_udf = F.udf(
        lambda age: "<30" if age < 30 else "30-60" if age <= 60 else ">60",
        StringType(),
    )
    df_with_udf = df_with_quarter.withColumn("age_group_udf", age_group_udf(F.col("age")))
    save_dataframe(df_with_udf.limit(200), reports_dir / "transformed_sample.csv")

    job_agg = (
        df.groupBy("job")
        .agg(
            F.avg("balance").alias("average_balance"),
            F.expr("percentile_approx(age, 0.5)").alias("median_age"),
        )
        .orderBy(F.desc("average_balance"))
    )
    save_dataframe(job_agg, reports_dir / "job_balance_and_median_age.csv")

    marital_yes = (
        df.filter(F.col("y") == "yes")
        .groupBy("marital")
        .count()
        .orderBy(F.desc("count"))
    )
    save_dataframe(marital_yes, reports_dir / "marital_subscription_counts.csv")

    education_subscription = (
        df.groupBy("education")
        .agg((F.avg(F.when(F.col("y") == "yes", 1).otherwise(0)) * 100).alias("subscription_rate"))
        .orderBy(F.desc("subscription_rate"))
    )
    save_dataframe(education_subscription, reports_dir / "education_subscription_rate.csv")

    default_rate = (
        df.groupBy("job")
        .agg((F.avg(F.when(F.col("default") == "yes", 1).otherwise(0)) * 100).alias("default_rate"))
        .orderBy(F.desc("default_rate"))
    )
    save_dataframe(default_rate.limit(3), reports_dir / "top3_job_default_rate.csv")

    month_contact_summary = (
        df.groupBy("month")
        .agg(
            F.count("*").alias("contact_count"),
            (F.avg(F.when(F.col("y") == "yes", 1).otherwise(0)) * 100).alias("success_rate"),
        )
        .withColumn("month_order", month_order_expr("month"))
        .orderBy("month_order")
        .drop("month_order")
    )
    save_dataframe(month_contact_summary, reports_dir / "month_contact_summary.csv")

    duration_by_target = df.groupBy("y").agg(F.avg("duration").alias("average_duration")).orderBy("y")
    save_dataframe(duration_by_target, reports_dir / "duration_by_subscription.csv")

    default_distribution = df.groupBy("default").count().orderBy("default")
    save_dataframe(default_distribution, reports_dir / "default_distribution.csv")

    contact_success = (
        df.groupBy("contact")
        .agg((F.avg(F.when(F.col("y") == "yes", 1).otherwise(0)) * 100).alias("success_rate"))
        .orderBy(F.desc("success_rate"))
    )
    save_dataframe(contact_success, reports_dir / "contact_success_rate.csv")

    # Correlation gives a quick numerical check before jumping to charts and grouped summaries.
    correlation_value = df.stat.corr("age", "balance")

    df_with_quarter.createOrReplaceTempView("bank_clients")
    spark_sql_avg_balance = spark.sql(
        """
        SELECT
            age_group,
            ROUND(AVG(balance), 2) AS avg_balance
        FROM (
            SELECT
                CASE
                    WHEN age < 30 THEN '<30'
                    WHEN age <= 60 THEN '30-60'
                    ELSE '>60'
                END AS age_group,
                balance
            FROM bank_clients
        )
        GROUP BY age_group
        ORDER BY avg_balance DESC
        """
    )
    save_dataframe(spark_sql_avg_balance, reports_dir / "spark_sql_avg_balance_by_age_group.csv")

    spark_sql_common_jobs = spark.sql(
        """
        SELECT job, COUNT(*) AS client_count
        FROM bank_clients
        GROUP BY job
        ORDER BY client_count DESC
        LIMIT 10
        """
    )
    save_dataframe(spark_sql_common_jobs, reports_dir / "spark_sql_top_jobs.csv")

    job_counts_pd = df.groupBy("job").count().orderBy(F.desc("count")).toPandas()
    plt.figure(figsize=(12, 6))
    sns.barplot(data=job_counts_pd, x="job", y="count", hue="job", palette="viridis", legend=False)
    plt.xticks(rotation=45, ha="right")
    plt.title("Client count by job type")
    plt.tight_layout()
    plt.savefig(figures_dir / "job_counts.png", dpi=200)
    plt.close()

    default_distribution_pd = default_distribution.toPandas()
    plt.figure(figsize=(6, 4))
    sns.barplot(data=default_distribution_pd, x="default", y="count", hue="default", palette="magma", legend=False)
    plt.title("Credit default distribution")
    plt.tight_layout()
    plt.savefig(figures_dir / "default_distribution.png", dpi=200)
    plt.close()

    summary_payload = {
        "age_balance_correlation": correlation_value,
        "highest_contact_month": month_contact_summary.orderBy(F.desc("contact_count")).first().asDict(),
        "best_contact_method": contact_success.first().asDict(),
    }

    ensure_dir(reports_dir)
    (reports_dir / "eda_summary.json").write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    print("EDA outputs saved to", reports_dir)
    print("EDA figures saved to", figures_dir)
    spark.stop()


if __name__ == "__main__":
    main()
