from etlcode.utils.sparkSessionBuilder import createSparkSession
from etlcode.utils.schemaBuild import *
from etlcode.utils.headerBuild import *
from etlcode.utils.ingestion import *
from etlcode.utils import jobTaskIDGen
from etlcode.utils import audit
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType

def deduplicate(rawDF):

    try:
        hashedDF = rawDF.withColumn("row_sha2", sha2(concat_ws("||", *rawDF.columns), 256))
        deDuplicatedDF = hashedDF.dropDuplicates(["row_sha2"])
        return deDuplicatedDF
    except :
        spark = createSparkSession()
        #Defining the schema of the dataframe.
        schema = StructType([StructField("dummy", IntegerType(), True)])

        #Creating an empty dataframe.
        empty_df = spark.createDataFrame([], schema)
        return empty_df


