from etlcode.utils.sparkSessionBuilder import createSparkSession
from etlcode.utils.schemaBuild import *
from etlcode.utils.headerBuild import *
from etlcode.utils.ingestion import *
from etlcode.utils import jobTaskIDGen
from etlcode.utils import audit


def ingestSrcData(jobID,dataDictConfigPath,dataDictMappingConfigPath,srcConfigPath,tableNm,sourceName,sourceType):

    spark = createSparkSession()
    taskID = jobTaskIDGen.genID()
    print(taskID)
    TaskName = 'ingestSrcData'
    audit.insertTaskAuditData(jobID,taskID,'TASK',TaskName,sourceType,tableNm)
    # # ddl_schema = sparkSchemaBuild(spark,dataDictConfigPath,dataDictMappingConfigPath,tableNm)
    # # print(ddl_schema)
    colNMs = sparkHeaderBuild(spark,dataDictConfigPath,tableNm)
    # print(colNMs)
    ingestion_func = globals()[sourceType+"_data_ingestion"]

    try:
        ingestionCount = ingestion_func(spark,srcConfigPath,sourceName,tableNm,colNMs,jobID)
        try:
            tmp = int(ingestionCount)
            audit.updateTaskAuditDataPass(jobID,taskID,'TASK',TaskName,sourceType,tableNm,ingestionCount)
        except Exception as error:
            audit.updateTaskAuditDataFail(jobID,taskID,'TASK',TaskName,sourceType,tableNm,tmp)
           
    # Do something
    except Exception as error:
            audit.updateTaskAuditDataFail(jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)
            print(error)
            