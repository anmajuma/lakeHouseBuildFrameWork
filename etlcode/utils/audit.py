from etlcode.utils.sparkSessionBuilder import createSparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
import datetime

jobcolumns = StructType([StructField('JobID',
                                StringType(), True),
                    StructField('JobName',
                                  StringType(), True),
                    StructField('SourceSystemName',
                                StringType(), True),
                     StructField('SourceTableName',
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


def insertJobAuditData(AuditItemId,AuditItemType,SourceSystemName,SourceTableName):
  root = "/home/animesh/python-project/lakeHouseBuildFrameWork/auditlog"
  spark = createSparkSession()
  # ct stores current time
  ct = datetime.datetime.now()
  if AuditItemType == 'JOB' :
    auditDeltaTbl = f"{root}/jobauditlog"
   
    data2 = [(AuditItemId,"daily_etl_job"+SourceSystemName+"_"+SourceTableName,SourceSystemName,SourceTableName,0,0,0,ct,ct,'Job Started','NA')]
    updates = spark.createDataFrame(data=data2,schema=jobcolumns)
    # updates.printSchema()
    # updates.show(truncate=False)
    dest = DeltaTable.forPath(spark, auditDeltaTbl)
    dest.alias("audit").merge(updates.alias("updates"),'audit.JobID = updates.JobID').whenNotMatchedInsertAll().execute()
  auditDF = spark.sql(f"select * from delta.`{auditDeltaTbl}`")
  auditDF.show(10, truncate=False)
  print(auditDF.count())

def insertTaskAuditData(JobID,AuditItemId,AuditItemType,TaskName,SourceSystemName,SourceTableName):
  root = "/home/animesh/python-project/lakeHouseBuildFrameWork/auditlog"
  spark = createSparkSession()
  # ct stores current time
  ct = datetime.datetime.now()
  if AuditItemType == 'TASK' :
    auditDeltaTbl = f"{root}/taskauditlog"  

    data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,0,0,'Task Started','NA')]
    updates = spark.createDataFrame(data=data2,schema=taskcolumns)
    # updates.printSchema()
    # updates.show(truncate=False)
    dest = DeltaTable.forPath(spark, auditDeltaTbl)
    dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenNotMatchedInsertAll().execute()
  
  auditDF = spark.sql(f"select * from delta.`{auditDeltaTbl}`")
  auditDF.show(10, truncate=False)

def updateTaskAuditDataPass(JobID,AuditItemId,AuditItemType,TaskName,SourceSystemName,SourceTableName,row_count):
  root = "/home/animesh/python-project/lakeHouseBuildFrameWork/auditlog"
  spark = createSparkSession()
  # ct stores current time
  ct = datetime.datetime.now()
  if AuditItemType == 'TASK' :
    auditDeltaTbl = f"{root}/taskauditlog"  

    if TaskName == 'ingestSrcData':
      data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,row_count,0,0,'Task Completed','NA')]
      updates = spark.createDataFrame(data=data2,schema=taskcolumns)
      # updates.printSchema()
    # updates.show(truncate=False)
      dest = DeltaTable.forPath(spark, auditDeltaTbl)
      dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "RowsIngested": "updates.RowsIngested" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()
    if TaskName.__contains__('cleanRawData'):
      print("Data Cleaning Task")
      data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,row_count,0,'Task Completed','NA')]
      updates = spark.createDataFrame(data=data2,schema=taskcolumns)
      # updates.printSchema()
    # updates.show(truncate=False)
      dest = DeltaTable.forPath(spark, auditDeltaTbl)
      dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "RowsRejected": "updates.RowsRejected" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()
    if TaskName == 'loadCleansedData':
      data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,row_count,0,'Task Completed','NA')]
      updates = spark.createDataFrame(data=data2,schema=taskcolumns)
      # updates.printSchema()
    # updates.show(truncate=False)
      dest = DeltaTable.forPath(spark, auditDeltaTbl)
      dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "RowsUpserted": "updates.RowsUpserted" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()


  auditDF = spark.sql(f"select * from delta.`{auditDeltaTbl}`")
  auditDF.show(10, truncate=False)
  print(auditDF.count())

def updateTaskAuditDataFail(JobID,AuditItemId,AuditItemType,TaskName,SourceSystemName,SourceTableName,error):
  root = "/home/animesh/python-project/lakeHouseBuildFrameWork/auditlog"
  spark = createSparkSession()
  # ct stores current time
  ct = datetime.datetime.now()
  if AuditItemType == 'TASK' :
    auditDeltaTbl = f"{root}/taskauditlog"  
    data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,0,0,'Task Failed',error)]
    updates = spark.createDataFrame(data=data2,schema=taskcolumns)
    updates.printSchema()
    dest = DeltaTable.forPath(spark, auditDeltaTbl)
    dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "ErrorMessage": "updates.ErrorMessage" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()
# select operation, 
#        timestamp, 
#        operationMetrics.numTargetRowsInserted,
#        operationMetrics.numTargetRowsUpdated
#   from (describe history delta.`your_table` limit 1)
#  where operation = 'MERGE'
