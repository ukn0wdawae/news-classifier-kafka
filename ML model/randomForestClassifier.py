from .mongoRetreive import retriveData
import pandas as pd
from pyspark.sql import dataframe as pysparkDataFrame
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import nltk
from nltk.corpus import stopwords
from pyspark.conf import SparkConf
from pyspark import SparkContext
from itertools import product
nltk.download('stopwords')
conf = SparkConf().set("spark.sql.broadcastTimeout", "36000") 
sc = SparkContext()
numCores = sc.defaultParallelism
print("number of cores", numCores)

spark = SparkSession.builder \
    .master('"spark://spark-master:7077"') \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps") \
    .appName('my-cool-app') \
    .getOrCreate()

hyperParametersRanges = {
    'numTrees': [10, 50, 100, 200],
    'maxDepth': [3, 4, 5, 10, 20],
    'minInstancesPerNode': [1, 2, 5, 10],
    'minInfoGain': [0.0, 0.1, 0.2, 0.3],
    'maxBins': [8, 16, 32, 64],
    'subsamplingRate': [0.5, 0.8, 1.0],
    'featureSubsetStrategy': ['auto', 'all', 'sqrt', 'log2', 'onethird'],
    'impurity': ['gini', 'entropy'],
    'cacheNodeIds': [False, True],
    'checkpointInterval': [10, 50, 100],
}


paramGrids = [dict(zip(hyperParametersRanges.keys(), values))
              for values in product(*hyperParametersRanges.values())]

def GetDataSet(fromFile: bool = False, fileName: str = "8CatNewsData.csv") -> pd.DataFrame:
    print("Pulling Data")
    if (fromFile):
        dataFrame = pd.read_csv(fileName)
    else:
        dataFrame = retriveData()
    print("Data pulled")
    dataFrame = dataFrame.dropna()
    return dataFrame

def ConvertTextToLowerCase(text: str) -> str:
    return text.lower()

def DataPipeline():
    regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
    stoppingWords = stopwords.words('english')
    stoppingWords = [ConvertTextToLowerCase(
        stoppingWord) for stoppingWord in stoppingWords] + ["http","https","amp","rt","t","c","the"] 
    stoppingWordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(stoppingWords)
    countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=0)
    stringIndexing = StringIndexer(inputCol = "category", outputCol = "label")
    pipeline = Pipeline(stages=[regexTokenizer, stoppingWordsRemover, countVectors, stringIndexing])
    return pipeline

def MapLabelandTopics(dataSet):
    labels = []
    mappedlabels = {}
    for data in dataSet.collect():
        if(data['label'] not in labels):
            labels.append(data['label'])
            mappedlabels[int(data['label'])]= data['category']
    return mappedlabels
def evaluate(model, testData):
    LRPredictions = model.transform(testData)
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    return evaluator.evaluate(LRPredictions)

def TrainModel(dataSet,paramGrid ):
    (trainingData, testData) = dataSet.randomSplit([0.7, 0.2], seed = 42)
    logisticRegression = RandomForestClassifier(*paramGrid)
    model = logisticRegression.fit(trainingData)
    accuracy = evaluate(model, testData)
    return model, accuracy

def FindBestRFCModel(paramGrids= paramGrids):
    dataSet = GetDataSet(fromFile=False)
    dataPipeline = DataPipeline()
    pipelineFit = dataPipeline.fit(dataSet)
    dataSet = pipelineFit.transform(dataSet)
    mappedLabels = MapLabelandTopics(dataSet)
    models = []
    accuracies = []
    for paramGrid in paramGrids:
        model, accuracy = TrainModel(dataSet, paramGrid)
        models.append(model)
        accuracies.append(accuracy)
        if (accuracy > 95):
            break
    model = model[accuracies.index(max(accuracies))]
    return model, dataPipeline, mappedLabels, max(accuracies)


def RFCModelPredict(model, dataPipeline, params):
    params = spark.createDataFrame(params)
    testData = dataPipeline.transform(params)
    RFCPredictions = model.transform(testData)
    return RFCPredictions.collect()[-1]['prediction']