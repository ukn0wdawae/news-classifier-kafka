import requests
import json
import os
from APITokens import *
import pandas as pd
 
def RapidAPIFeed() -> list:
    apiData = json.load(open("./Producer/APIData.json", 'r'))
    for key in apiData['RapidAPI']:
        numberOfAPIs = len(apiData['RapidAPI'][key]['api'])
        maxAPIS = apiData['RapidAPI'][key]['MaxAPIS']
        eachApiHits = (maxAPIS)//numberOfAPIs
        print(numberOfAPIs, maxAPIS,eachApiHits)
        j=0
        for api in apiData['RapidAPI'][key]['api']:
            j=j+1
            api['headers']["X-RapidAPI-Key"] = RAPIDAPI_TOKEN
            for i in range(eachApiHits):
                # print(api['requestType'], api['url'],api['headers'],api["queryString"])
                response = requests.request(api['requestType'], api['url'], headers=api['headers'], params=api["queryString"])
                try:
                    json_response = response.json()
                    json.dump(json_response,open("{}-{}.json".format(j,i),'w'))

                except:
                    with open("{}-{}.txt".format(j,i),'w') as f:
                        f.write(response)

# RapidAPIFeed()

def NYAPIFeed():
    with open("./fault files.txt", 'r') as f:
        files = f.readlines()
    
    for file in files:
        [year, month] = file.split("/")[-1].split(".")[0].split('-')
        print(file,year, month )
        url = "https://api.nytimes.com/svc/archive/v1/{}/{}.json?api-key={}".format(year,month,NYTIMES_TOKEN)
        response = requests.request("GET", url)
        try:
            json_response = response.json()
            json.dump(json_response,open("./Data/NYTimeData/{}-{}.json".format(year,month),'w'))

        except Exception as e:
            print(e)
            with open("./Data/NYTimeData/{}-{}.json".format(year,month),'w') as f:
                f.write(str(response))
    pass
# NYAPIFeed()

