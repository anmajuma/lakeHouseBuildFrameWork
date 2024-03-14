
from etlcode.utils import jobTaskIDGen
from etlcode.tasks import src2landing ,landing2brnz
from etlcode.utils import audit


jobID = jobTaskIDGen.genID()
print(jobID)
dataDictConfigPath = "lakeHouseBuildFrameWork/etl-config/datadict.json"
dataDictMappingConfigPath = "lakeHouseBuildFrameWork/etl-config/mongoToSparkMapping.json"
srcConfigPath = "lakeHouseBuildFrameWork/etl-config/mongo_config.json"
brnzPath = "lakeHouseBuildFrameWork/bronze"
landingPath ="lakeHouseBuildFrameWork/landing"
tableNm = "Customers"
sourceName = "WideWorldImporters"
sourceType = "mongodb"
audit.insertJobAuditData(jobID,'JOB',sourceType,tableNm)

src2landing.ingestSrcData(jobID,dataDictConfigPath,dataDictMappingConfigPath,srcConfigPath,tableNm,sourceName,sourceType)
landing2brnz.createBrnzTbl(jobID,brnzPath,landingPath,tableNm,sourceName,sourceType)