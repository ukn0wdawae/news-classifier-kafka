import requests

# Fetch articles related to the provided query string from RapidAPI news API
def GetRapidAPIFeed(queryString):
    attributeList = ["title","published_date","link","clean_url","summary","media","topic"]
    url = "https://free-news.p.rapidapi.com/v1/search"
    querystring = {"q":queryString, "lang":"en"}
    headers = {
    'x-rapidapi-host': "free-news.p.rapidapi.com",
    'x-rapidapi-key': ""
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    json_response = response.json()
    articleList = []
    try:
        for article in json_response["page_size"]:
            articleData = {}
            for items in attributeList:
                articleData[items] = article[items]
            articleList.append(articleData)
        return articleList
    except:
        return None


# Fetch articles for each query string in the provided list from RapidAPI news API
def GetRapidAPIData(queryStringList):
    articleList = []
    for queryString in queryStringList:
        articles = GetRapidAPIFeed(queryString)
        if(articles):
            articleList.append(articles)
    return articleList
        
