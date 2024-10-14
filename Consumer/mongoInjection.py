from pymongo import MongoClient

def InjectToMongodb(articleList: list) -> bool:
    try:
        client = MongoClient(
            "")

        collection = client.bigdatanewsclassification.news
        for article in articleList:
            if(not collection.find_one(article)):
                collection.insert_one(article)
        return True
    except:
        return False
