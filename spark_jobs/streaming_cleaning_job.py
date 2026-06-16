from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, from_json, lit, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "transactions"

CLEAN_OUTPUT_PATH = Path("output/clean_data")
BAD_OUTPUT_PATH = Path("output/bad_records")
CHECKPOINT_PATH = Path("output/checkpoints/transactions_quality")
HEALTH_REPORT_PATH = Path("output/health_report/latest")


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("city", StringType(), True),
        StructField("device", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("status", StringType(), True),
    ]
)


def add_quality_columns(records_df):
    return records_df.withColumn(
        "error_reason",
        when(col("transaction_id").isNull(), lit("missing_transaction_id"))
        .when(col("user_id").isNull(), lit("missing_user_id"))
        .when(col("email").isNull() | ~col("email").contains("@"), lit("invalid_email"))
        .when(col("amount").isNull() | (col("amount") <= 0), lit("invalid_amount"))
        .when(col("event_time").isNull(), lit("missing_event_time"))
        .otherwise(lit("valid")),
    ).withColumn("processed_at", current_timestamp())


def write_health_report(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    health_report_df = (
        batch_df.groupBy("error_reason")
        .count()
        .withColumn("pipeline_name", lit("mini-kafka-pyspark-docker-pipeline"))
        .withColumn("batch_id", lit(batch_id))
        .withColumn("report_time", current_timestamp())
        .withColumn("quality_status", expr("case when error_reason = 'valid' then 'clean' else 'bad' end"))
    )

    health_report_df.coalesce(1).write.mode("overwrite").json(str(HEALTH_REPORT_PATH))


def main():
    CLEAN_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    BAD_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)
    HEALTH_REPORT_PATH.mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("MiniKafkaPySparkDockerPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("record"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("record.*", "kafka_timestamp")

    quality_df = add_quality_columns(parsed_df)
    clean_df = quality_df.filter(col("error_reason") == "valid").drop("error_reason")
    bad_df = quality_df.filter(col("error_reason") != "valid")

    clean_df.writeStream.format("json").option("path", str(CLEAN_OUTPUT_PATH)).option(
        "checkpointLocation", str(CHECKPOINT_PATH / "clean_data")
    ).outputMode("append").start()

    bad_df.writeStream.format("json").option("path", str(BAD_OUTPUT_PATH)).option(
        "checkpointLocation", str(CHECKPOINT_PATH / "bad_records")
    ).outputMode("append").start()

    quality_df.writeStream.foreachBatch(write_health_report).option(
        "checkpointLocation", str(CHECKPOINT_PATH / "health_report")
    ).outputMode("append").start()

    print("Streaming data quality job started.")
    print(f"Reading Kafka topic: {TOPIC}")
    print(f"Writing clean records to: {CLEAN_OUTPUT_PATH}")
    print(f"Writing bad records to: {BAD_OUTPUT_PATH}")
    print(f"Writing latest health report to: {HEALTH_REPORT_PATH}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
