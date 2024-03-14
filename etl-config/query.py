import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import shutil
import os
from pprint import pprint
from pyspark.sql.types import *

root = "/home/animesh/python-project/lakeHouseBuildFrameWork/auditlog"
auditDeltaTbl = f"{root}/taskauditlog"  
# shutil.rmtree(root, ignore_errors=True)
#source = "hdfs://192.168.1.103:54310/delta"
builder = (
  SparkSession.builder
    .appName('producer')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

auditDF = spark.sql(f"select * from delta.`{auditDeltaTbl}`")

auditDF.show(truncate=False)