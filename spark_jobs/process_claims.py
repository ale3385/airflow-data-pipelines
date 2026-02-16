"""PySpark job for processing raw claims data on EMR Serverless."""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DateType


SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("provider_npi", StringType(), True),
        StructField("claim_type", StringType(), True),
        StructField("service_date", DateType(), True),
        StructField("billed_amount_cents", LongType(), True),
        StructField("diagnosis_codes", StringType(), True),
        StructField("procedure_code", StringType(), True),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-prefix", required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--file-count", type=int, default=0)
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("claims-processing")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    df = spark.read.schema(SCHEMA).json(args.input_prefix)

    transformed = (
        df.filter(F.col("claim_id").isNotNull())
        .withColumn("claim_type", F.upper(F.trim(F.col("claim_type"))))
        .withColumn("billed_amount", F.round(F.col("billed_amount_cents") / 100, 2))
        .withColumn("diagnosis_code_array", F.split(F.col("diagnosis_codes"), ","))
        .withColumn("primary_diagnosis", F.element_at(F.col("diagnosis_code_array"), 1))
        .withColumn("diagnosis_count", F.size(F.col("diagnosis_code_array")))
        .withColumn("processing_date", F.current_date())
        .drop("diagnosis_codes", "diagnosis_code_array", "billed_amount_cents")
    )

    deduped = transformed.dropDuplicates(["claim_id"])

    (deduped.repartition(10).write.mode("overwrite").parquet(args.output_prefix))

    row_count = deduped.count()
    print(f"Processed {row_count} claims to {args.output_prefix}")

    spark.stop()


if __name__ == "__main__":
    main()
