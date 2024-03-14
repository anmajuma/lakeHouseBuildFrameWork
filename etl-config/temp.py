import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import shutil
import os
from pprint import pprint
from pyspark.sql.types import *

root = "/home/animesh/python-project/lakeHouseBuildFrameWork/auditlog"
# shutil.rmtree(root, ignore_errors=True)
#source = "hdfs://192.168.1.103:54310/delta"
builder = (
  SparkSession.builder
    .appName('producer')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()


shutil.rmtree(root, ignore_errors=True)
# Create an expected schema
jobcolumns = StructType([StructField('JobName',
                                  StringType(), True),
                    StructField('SourceSystemName',
                                StringType(), True),
                     StructField('SourceTableName',
                                StringType(), True),
                    StructField('JobID',
                                StringType(), True),
                    StructField('RowsIngested',
                                IntegerType(), True),   
                    StructField('RowsRejected',
                                IntegerType(), True),      
                    StructField('RowsUpserted',
                                IntegerType(), True),                                 
                    StructField('JobStartTime',
                                TimestampType(), True),
                    StructField('JobEndTime',
                                TimestampType(), True),
                    StructField('JobStatus',
                                StringType(), True),      
                    StructField('ErrorMessage',
                                StringType(), True)                                                                                                                                                                                                                                                                                                                                                               
                                ])



# Create an expected schema
taskcolumns = StructType([StructField('JobID',
                                  StringType(), True),
                    StructField('TaskName',
                                StringType(), True),
                    StructField('TaskID',
                                StringType(), True),
                    StructField('TaskStartTime',
                                TimestampType(), True),
                    StructField('TaskEndTime',
                                TimestampType(), True),                                                                                                
                    StructField('RowsIngested',
                                IntegerType(), True),   
                    StructField('RowsRejected',
                                IntegerType(), True),      
                    StructField('RowsUpserted',
                                IntegerType(), True),      
                    StructField('TaskStatus',
                                StringType(), True),      
                    StructField('ErrorMessage',
                                StringType(), True)                                                                                                                                                                                                                                                                                                                                                               
                                ])
 

jobdf = spark.read.option("header",True).csv("lakeHouseBuildFrameWork/etlcode/utils/dummyJob.csv",schema = jobcolumns)
taskdf = spark.read.option("header",True).csv("lakeHouseBuildFrameWork/etlcode/utils/dummyTask.csv",schema = taskcolumns)

# Print the dataframe
print('Dataframe :')
jobdf.show()
taskdf.show()
jobdf.write.format("delta").mode("overwrite").save(root+'/jobauditlog')
taskdf.write.format("delta").mode("overwrite").save(root+'/taskauditlog')