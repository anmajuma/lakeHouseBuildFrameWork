import pymongo
import json
from bson.json_util import dumps
import pandas as pd
from pandas import json_normalize
from pyspark.sql import functions as F


def mongodb_data_ingestion(spark,mongoConfigPath,sourceName,tableNm,colNMs,jobID):
    f=open(mongoConfigPath)
    data = json.load(f)
    f.close()
    # print(data["conn"])
    for connItems in data["conn"]:
        for (k, v) in connItems.items():
            globals()[k] = str(v)
    mongo_con_url = "mongodb+srv://"+user+":"+password+"@"+url+"/"+sourceName+"?retryWrites=true&w=majority"
    # print(mongo_con_url)
    try:
        client=pymongo.MongoClient(mongo_con_url)
        mongodb=client[sourceName]
        collectionList = [tableNm]
        for x in collectionList:
            print(x)
            collectNm = mongodb[x]
            cursor = collectNm.find()
            pdf = json_normalize(cursor)
            pdf = pdf.drop(['_id'], axis=1)
        # print(len(pdf))
            pdf1 = pd.DataFrame(pdf, columns=colNMs)
            sparkDF = spark.createDataFrame(pdf1)
            df =sparkDF.withColumn('IngestTimeStamp', F.current_timestamp()).withColumn("batch_id",F.lit(jobID))
        # print(df.count())
            landingPath = "lakeHouseBuildFrameWork/landing/mongodb/"+sourceName+"/"+tableNm+"/"
            print(landingPath)
            df = df.withColumn("part_date", F.current_date())
            df.write.mode("overwrite").format("parquet").partitionBy("part_date").save(landingPath)
            return df.count()
    except Exception as Error:   
            print(Error)     
            return Error
    