import xgboost as xgb
import pandas as pd
import nltk
from nltk.corpus import stopwords
from .mongoRetreive import retriveData
from sklearn.pipeline import Pipeline
from nltk.tokenize import RegexpTokenizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.metrics import classification_report
import os
from time import time
from itertools import product

os.environ['CUDA_VISIBLE_DEVICES'] = '0'
nltk.download('stopwords')
hyperParametersRanges = {
    'objective': ['multi:softmax'],
    'tree_method': ['gpu_hist'],
    'num_class': [8],
    'n_jobs': [-1],
    'verbosity': [1],
    'max_depth': [3, 4, 5],
    'learning_rate': [0.1, 0.2,0.3,0.4,0.5],
    'subsample': [0.5, 0.8],
    'colsample_bytree': [0.5, 0.8],
    'gamma': [0, 0.1],
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

def GetStoppingWords() -> list:
    stoppingWords = stopwords.words('english')
    stoppingWords_ = [ConvertTextToLowerCase(
        stoppingWord) for stoppingWord in stoppingWords]
    return stoppingWords_

class RegTokenizer(BaseEstimator, TransformerMixin):
    def __init__(self, pattern=r'\w+', gaps=False, discard_empty=True):
        self.tokenizer = RegexpTokenizer(
            pattern=pattern, gaps=gaps, discard_empty=discard_empty)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return [self.tokenizer.tokenize(text) for text in X]

def tokenizer(text):
    tokenizer = RegTokenizer()
    return tokenizer.transform([text])[0]

def DataPipeline() -> Pipeline:
    vectorizer = TfidfVectorizer(
        tokenizer=tokenizer, stop_words=GetStoppingWords())
    pipeline = Pipeline(
        steps=[
            ('vectorizer', vectorizer),
        ])
    return pipeline

def LabelEncoding(outputColumn: pd.DataFrame) -> pd.DataFrame:
    labelEncoding_ = LabelEncoder()
    labelEncoding_.fit(outputColumn)
    encodedOutputColumn = labelEncoding_.transform(outputColumn)
    return encodedOutputColumn, labelEncoding_

def DataPreProcessing(inputColumn:  pd.DataFrame, outputColumn: pd.DataFrame = None) -> tuple:
    dataPipeline = DataPipeline()
    dataPipeline = dataPipeline.fit(inputColumn)
    inputColumn = dataPipeline.transform(inputColumn)
    if (type(outputColumn) != 'NoneType'):
        outputColumn, labelEncoding = LabelEncoding(outputColumn)
    return inputColumn, outputColumn, labelEncoding, dataPipeline

def GetCategoryCount(dataSet: pd.DataFrame) -> None:
    dataSet['categoryId'] = dataSet['category'].factorize()[0]
    category = dataSet[['category', 'categoryId']
                       ].drop_duplicates().sort_values('categoryId')
    print(dataSet.groupby('category').categoryId.value_counts())

def CreateBatches(inputColumn, outputColumn):
    batchSize = 10000
    numBatches = len(outputColumn) // batchSize + 1
    dMatrices = []
    for i in range(numBatches):
        startIndex = i*batchSize
        endIndex = startIndex+batchSize
        if (endIndex > len(outputColumn)):
            endIndex = len(outputColumn)
        textData = inputColumn[startIndex:endIndex]
        lableData = outputColumn[startIndex:endIndex]
        dMatrices.append([textData, lableData])
    return dMatrices

def TrainModel(inputColumn, outputColumn, paramGrid: dict):
    X_train, X_test, y_train, y_test = train_test_split(
        inputColumn, outputColumn, test_size=0.2, random_state=42)
    del inputColumn
    del outputColumn
    dMatrices = CreateBatches(X_train, y_train)
    del X_train
    del y_train
    i = 0
    totalTime = 0
    model = None
    for dMatrix in dMatrices:
        i += 1
        startTime = time()
        dtrain = xgb.DMatrix(dMatrix[0], label=dMatrix[1])
        if (model != None):
            model = xgb.train(paramGrid, dtrain,
                              num_boost_round=1, xgb_model=model)
        else:
            model = xgb.train(paramGrid, dtrain, num_boost_round=10)
        endTime = time()
        timeTaken = endTime - startTime
        totalTime += timeTaken
        test_pred = model.predict(xgb.DMatrix(X_test))
        accuracy = accuracy_score(y_test, test_pred)
        report = classification_report(y_test, test_pred)
    return model, accuracy, report

def FindBestXGBoostModel(paramGrids= paramGrids):
    dataSet = GetDataSet(fromFile=False)
    GetCategoryCount(dataSet)
    inputColumn = dataSet['text']
    outputColumn = dataSet['category']
    del dataSet
    print("PreProcessing the Data")
    inputColumn, outputColumn, labelEncoding, dataPipeline = DataPreProcessing(inputColumn, outputColumn)
    models = []
    accuracies = []
    reports = []
    for paramGrid in paramGrids:
        model, accuracy, report = TrainModel(inputColumn, outputColumn, paramGrid)
        models.append(model)
        accuracies.append(accuracy)
        reports.append(report)
        if (accuracy > 95):
            break
    model = model[accuracies.index(max(accuracies))]
    report = reports[accuracies.index(max(accuracies))]
    return model, dataPipeline, labelEncoding, max(accuracies)


def XGBoostModelPredict(model,dataPipeline, labelEncoding, query ):
    dataSet = pd.DataFrame([query],columns=['text'])
    inputColumn = dataPipeline.transform(dataSet)
    inputColumn = xgb.DMatrix(inputColumn)
    test_pred = model.predict(inputColumn)
    print(test_pred)

