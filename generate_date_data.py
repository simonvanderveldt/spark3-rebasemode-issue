#!/usr/bin/env python
import os
from datetime import date

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, DateType, StringType

VENV = os.environ["VIRTUAL_ENV"].split("/")[-1].split(".")[-1]

spark_conf = SparkConf().setAll([
    ("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")])
spark = SparkSession.builder.appName("read-data").config(conf=spark_conf).getOrCreate()

# spark = SparkSession.builder.appName("generate-date-data").getOrCreate()
print("Spark version:", spark.version)
print("Spark conf", SparkConf().getAll())

schema = StructType([
    StructField("row", StringType(), True),
    StructField("date", DateType(), True)])

df = spark.createDataFrame([
    (1, date(day=1, month=10, year=1000)),
    (2, date(day=1, month=10, year=1880)),
    (3, date(day=1, month=10, year=2020))], schema)

df.printSchema()
df.show()

path = f"output/date{VENV}/"
print("Writing parquet files to", path)
df.repartition(1).write.mode("overwrite").parquet(path)
print("done")
