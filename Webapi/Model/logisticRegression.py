from .mongoRetreive import retriveData
import pandas as pd
from pyspark.sql import dataframe as pysparkDataFrame
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import nltk
from nltk.corpus import stopwords
from pyspark.conf import SparkConf
from pyspark import SparkContext
from itertools import product
nltk.download('stopwords')

# Initialize Spark configuration and session
conf = SparkConf().set("spark.sql.broadcastTimeout", "36000") 

sc = SparkContext.getOrCreate()
numCores = sc.defaultParallelism
print("number of cores", numCores)

spark = SparkSession.builder \
    .master('"spark://spark-master:7077"') \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps") \
    .appName('my-cool-app') \
    .getOrCreate()

# Define hyperparameter ranges for model tuning
hyperParametersRanges = {
    'featuresCol': ['features'], 
    'labelCol' : ['label'],
    'maxIter': [10, 50, 100, 200],
    'regParam': [0.0, 0.01, 0.1, 1.0],
    'elasticNetParam': [0.0, 0.5, 1.0],
    'tol': [1e-6, 1e-4, 1e-2],
    'fitIntercept': [True, False],
    'standardization': [True, False],
    'aggregationDepth': [2, 3, 4],
    'family': ['auto', 'binomial', 'multinomial'],
}

paramGrids = [dict(zip(hyperParametersRanges.keys(), values))
              for values in product(*hyperParametersRanges.values())]


# Load dataset either from a CSV file or MongoDB
def GetDataSet(fromFile: bool = False, fileName: str = "NewsData.csv") -> pd.DataFrame:
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

# Define the data preprocessing pipeline for tokenizing, removing stopwords, and feature extraction
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
    predictions = model.transform(testData)
    evaluatorAccuracy = MulticlassClassificationEvaluator(labelCol="label",predictionCol="prediction")
    evaluatorPrecision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    evaluatorRecall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
    evaluatorF1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    accuracy = evaluatorAccuracy.evaluate(predictions)
    precision = evaluatorPrecision.evaluate(predictions)
    recall = evaluatorRecall.evaluate(predictions)
    f1Score = evaluatorF1.evaluate(predictions)
    with open("./results1.txt", 'w') as f:
        f.write(",".join([ str(i) for i in [accuracy, precision, recall, f1Score]]))

    return [accuracy, precision, recall, f1Score]

def TrainModel(dataSet,paramGrid ):
    (trainingData, testData) = dataSet.randomSplit([0.7, 0.2], seed = 42)
    logisticRegression = LogisticRegression(**paramGrid)
    model = logisticRegression.fit(trainingData)
    [accuracy, precision, recall, f1Score] = evaluate(model, testData)
    return model, [accuracy, precision, recall, f1Score]

def FindBestLRModel(paramGrids= paramGrids):
    dataSet = GetDataSet(fromFile=False)
    dataSet = spark.createDataFrame(dataSet)
    dataPipeline = DataPipeline()
    pipelineFit = dataPipeline.fit(dataSet)
    dataSet = pipelineFit.transform(dataSet)
    mappedLabels = MapLabelandTopics(dataSet)
    models = []
    accuracies = []
    precisions = []
    recalls= []
    f1Scores= []
    for paramGrid in paramGrids:
        try:
            model, [accuracy, precision, recall, f1Score] = TrainModel(dataSet, paramGrid)
            models.append(model)
            accuracies.append(accuracy)
            precisions.append(precision)
            recalls.append(recall)
            f1Scores.append(f1Score)
            break
            if (accuracy > 95):
                break
        except:
            pass
    model = models[accuracies.index(max(accuracies))]
    precision = precisions[accuracies.index(max(accuracies))]
    recall = recalls[accuracies.index(max(accuracies))]
    f1Scores = f1Scores[accuracies.index(max(accuracies))]
    return model, dataPipeline, mappedLabels, max(accuracies), precision, recall, f1Scores


def LRModelPredict(model, dataPipeline, params):
    params = spark.createDataFrame(params)
    testData = dataPipeline.transform(params)
    LRPredictions = model.transform(testData)
    return LRPredictions.collect()[-1]['prediction']
LRModel1,LRDataPipeline, LRMappedLabels, LRAccuracy, LRPrecision, LRRecall, LRF1Scores = FindBestLRModel()