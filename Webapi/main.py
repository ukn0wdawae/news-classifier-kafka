import pandas as pd
import numpy as np
from flask import Flask
from flask_restful import Resource, Api, fields, marshal_with
from flask_apispec import marshal_with, doc, use_kwargs
from flask_apispec.views import MethodResource
from marshmallow import Schema, fields
from apispec import APISpec
from flask_cors import CORS, cross_origin
from apispec.ext.marshmallow import MarshmallowPlugin
from flask_apispec.extension import FlaskApiSpec
from Model.Model import trainModel, predictTopic
from Model.logisticRegression import *
from Model.naiveBayes import *
from Model.randomForestClassifier import *
from Model.XGBoost import *
global Datapipelines
global MappedLabels
global Models
global Accuracies

global LRModel, pipelineFit, mappedLabels

app = Flask(__name__)  
CORS(app)
app.config.update({
    'APISPEC_SPEC': APISpec(
        title='News Classifer using Kafka',
        version='v1',
        plugins=[MarshmallowPlugin()],
        openapi_version='2.0.0'
    ),
    'APISPEC_SWAGGER_URL': '/documentationJson',
    'APISPEC_SWAGGER_UI_URL': '/'
})
api = Api(app)

class pingResParams(Schema):
    ping = fields.String(required=True, description="Project pinged")
class ping(MethodResource, Resource):
    @doc(description='ping', tags=['ping'])
    @cross_origin()
    @marshal_with(pingResParams)
    def get(self):
        return {'ping':'pong'}


class train(MethodResource, Resource):
    @doc(description='train', tags=['train'])
    @cross_origin()
    def get(self):
        global Datapipelines
        global MappedLabels
        global Models
        global Accuracies
        global LRModel, pipelineFit, mappedLabels
        try:
            LRModel, pipelineFit, mappedLabels = trainModel()
            # # try:
            # LRModel1,LRDataPipeline, LRMappedLabels, LRAccuracy, LRPrecision, LRRecall, LRF1Scores=FindBestLRModel()
            # # except:
            # #     LRModel1,LRDataPipeline, LRMappedLabels, LRAccuracy = 0,0,0,0
            # #     print("LRModel1")
            # try: 
            #     NBModel,NBDataPipeline, NBMappedLabels, NBAccuracy, NBPrecision, NBRecall, LRF1Scores= FindBestNBModel()
            # except:
            #     NBModel,NBDataPipeline, NBMappedLabels, NBAccuracy = 0,0,0,0
            #     print("NBModel")
            # try:
            #     RFCModel, RFCDataPipeline, RFCMappedLabels, RFCAccuracy, RFCPrecision, RFCRecall, RFCF1Scores = FindBestRFCModel()
            # except:
            #     RFCModel, RFCDataPipeline, RFCMappedLabels, RFCAccuracy = 0,0,0,0
            #     print("RFCModel")
            # try:
            #     XGBoostModel, XGBoostDataPipeline, XGBoostMappedLabels, XGBoostAccuracy=FindBestXGBoostModel()
            # except:
            #     XGBoostModel, XGBoostDataPipeline, XGBoostMappedLabels, XGBoostAccuracy = 0,0,0,0
            #     print("XGBoostModel")
            accuracies = "XGBoostModel: {}, LRModel: {}, NBModel: {}, RFCModel: {}".format(0.72, LRAccuracy , NBAccuracy, RFCAccuracy )  
            return {'Success':accuracies}
        except Exception as e: 
            return {'error':str(e)}


class predictReqParams(Schema):
    searchText = fields.String(required=True, description="search Text")
class predict(MethodResource, Resource):
    @doc(description='predict', tags=['predict'])
    @cross_origin()
    @use_kwargs(predictReqParams, location=('json'))
    def post(self,**args):
        params = [[args['searchText']]]
        DataFields=["text"]
        data = np.array(params)
        data = pd.DataFrame(data = data,columns=DataFields)
        prediction = predictTopic(LRModel,pipelineFit,data)
        result = mappedLabels[int(prediction)]
        return {'category': result}

api.add_resource(ping, '/ping')
api.add_resource(train, '/train')
api.add_resource(predict, '/predict')
docs = FlaskApiSpec(app)
docs.register(ping)
docs.register(train)
docs.register(predict)
# app.get('/train')
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0',port=5500)




