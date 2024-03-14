from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def createSparkSession():
#source = "hdfs://192.168.1.103:54310/delta"
    builder = (
    SparkSession.builder
        .appName('lakeHouseApp')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark