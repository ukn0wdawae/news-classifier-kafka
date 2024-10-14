import requests
from bs4 import BeautifulSoup
import pandas as pd


# Fetches articles from a collection of RSS feed URLs categorized by topics
def GetRssFeed(resources: object) -> list:
    articles = []
    for category in resources:
        if(type(resources[category]) == list):
            for link in resources[category]:
               articles += GetArticles(category,link) # Handles multiple links for a category
        else:
            articles += GetArticles(category,resources[category])  # Handles a single link for a category
    return articles
           

# Retrieves articles from a given RSS feed URL and extracts relevant text data
def GetArticles(category:str, url: str)-> list:
    res = requests.get(url) 
    content = BeautifulSoup(res.content, features='xml') # Parse RSS content using XML parser
    articles = content.findAll('item') # Extract all articles (items) from the RSS feed
    articleData = []
    for article in articles:
        try:
            title = article.find('title').text
            description = article.find('description').text
            text = title + description
            text = CleanArticle(text)# Clean HTML tags from the combined text
            articleData.append({"text":text,"category":category} )
        except:
            pass # Skip any article that raises an error (e.g., missing title or description)
    return articleData

 #Removes HTML tags in text
def CleanArticle(text: str) -> str:  
    cleanText = ""
    i = 0
    while (i in range(len(text))):
        if(text[i] == '<'):
            while(text[i] != '>'):
                i+=1
            i+=1
        else:
           cleanText+= text[i]
           i+=1
    return cleanText



#example usage
# article  = GetCNNFeed()
# article = GetRssFeed(CNNResources)
# article += GetRssFeed(NewYorkTimeResources)
# article += GetRssFeed(TheGuardianResources)
# data = pd.DataFrame(article, columns=['text','category'])
# with open("test.csv",'w') as file:
#     data.to_csv(file)
