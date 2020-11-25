#!/usr/bin/env python
import os
import sys
from datetime import date

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, TimestampType, DateType, StringType

if len(sys.argv) == 3:
    spark_conf = SparkConf().setAll([
        ("spark.sql.legacy.parquet.datetimeRebaseModeInRead", sys.argv[2])])
    spark = SparkSession.builder.appName("read-data").config(conf=spark_conf).getOrCreate()
else:
    spark = SparkSession.builder.appName("read-data").getOrCreate()

print("Spark version:", spark.version)
print("Spark conf", SparkConf().getAll())

path = f"output/{sys.argv[1]}/*.parquet"
print("Reading parquet files from", path)

df = spark.read.parquet(path)

df.printSchema()
df.show()
print("done")
