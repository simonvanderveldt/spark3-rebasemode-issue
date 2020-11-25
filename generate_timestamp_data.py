#!/usr/bin/env python
import os
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

VENV = os.environ["VIRTUAL_ENV"].split("/")[-1].split(".")[-1]

spark = SparkSession.builder.appName("generate-timestamp-data").getOrCreate()
print("Spark version:", spark.version)
print("Spark conf", SparkConf().getAll())

schema = StructType([
    StructField("row", StringType(), True),
    StructField("timestamp", TimestampType(), True)])

df = spark.createDataFrame([
    (1, datetime(day=1, month=10, year=220, hour=10, minute=10, second=10)),
    (2, datetime(day=1, month=10, year=1880, hour=10, minute=10, second=10)),
    (3, datetime(day=1, month=10, year=2020, hour=10, minute=10, second=10))], schema)

df.printSchema()
df.show()

path = f"output/timestamp{VENV}/"
print("Writing parquet files to", path)
df.repartition(1).write.mode("overwrite").parquet(path)
print("done")
