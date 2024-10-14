import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np

def GetFromMongodb(filter: object) -> pymongo.cursor.Cursor:
    client = MongoClient(
                    "")
    collection = client.bigdatanewsclassification.news
    cursor = collection.find(filter)
    print(type(cursor))
    return cursor

def SaveDataPandas(dataList: list, DataFields: list) -> pd.DataFrame:
    arrayData = []
    for data in dataList:
        filteredData = []
        for field in DataFields:
            if(data[field]):
                filteredData.append(data[field])
            else:
                filteredData.append("")
        arrayData.append(filteredData)

    return pd.DataFrame(data=arrayData, columns=DataFields)

def retriveData(DataFields: list = ["text", "category"]) -> pd.DataFrame:
    dataList = GetFromMongodb({})
    return SaveDataPandas(dataList, DataFields)

# data = retriveData()
# with open("train.csv",'w') as file:
#     data.to_csv(file)
