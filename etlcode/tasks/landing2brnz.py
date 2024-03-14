from etlcode.utils.sparkSessionBuilder import createSparkSession
from etlcode.utils.schemaBuild import *
from etlcode.utils.headerBuild import *
from etlcode.utils.deduplicate import *
from etlcode.utils import jobTaskIDGen
from etlcode.utils import audit

def createBrnzTbl(jobID,brnzPath,landingPath,tableNm,sourceName,sourceType):

    spark = createSparkSession()
    taskID = jobTaskIDGen.genID()
    print(taskID)
    TaskName = 'cleanRawData_removeDuplicate'
    audit.insertTaskAuditData(jobID,taskID,'TASK',TaskName,sourceType,tableNm)
    rawDF = spark.read.format("parquet").load(landingPath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
    ingestedRowCount = rawDF.count()
    deDuplicatedDF = deduplicate(rawDF)
    try:
            deDuplicatedDF = deduplicate(rawDF)
            cleanedRowCount = deDuplicatedDF.count()
            rejectedRowCount = ingestedRowCount - cleanedRowCount
            deDuplicatedDF.write.format("delta").mode("append").partitionBy("part_date").save(brnzPath+'/'+sourceType+'/'+sourceName+'/'+tableNm)
            audit.updateTaskAuditDataPass(jobID,taskID,'TASK',TaskName,sourceType,tableNm,rejectedRowCount)
    except Exception as error:
            audit.updateTaskAuditDataFail(jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)