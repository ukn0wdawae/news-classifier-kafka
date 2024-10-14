from RapidAPIFeed import GetRapidAPIFeed
from json import dumps
from kafka import KafkaProducer
import time
from RssFeed import GetRssFeed
from APIFeed import *


CNNResources = {
    "business" :"http://rss.cnn.com/rss/money_latest.rss",
    "politics": "http://rss.cnn.com/rss/cnn_allpolitics.rss",
    "tech":"http://rss.cnn.com/rss/cnn_tech.rss",
    "health":"http://rss.cnn.com/rss/cnn_health.rss",
    "entertainment": "http://rss.cnn.com/rss/cnn_showbiz.rss"
}

ReutersResources = {
    "business" :"https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
    "politics": "https://www.reutersagency.com/feed/?best-topics=political-general&post_type=best",
    "tech":"https://www.reutersagency.com/feed/?best-topics=tech&post_type=best",
    "health":"https://www.reutersagency.com/feed/?best-topics=health&post_type=best",
    "entertainment": "https://www.reutersagency.com/feed/?best-topics=lifestyle-entertainment&post_type=best",
    "sports": "https://www.reutersagency.com/feed/?best-topics=sports&post_type=best",

}
NewYorkTimeResources = {
    "world": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "business" :"https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "politics": "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
    "tech":"https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
    "health":"https://rss.nytimes.com/services/xml/rss/nyt/Health.xml",
    "entertainment": ["https://rss.nytimes.com/services/xml/rss/nyt/Movies.xml","https://rss.nytimes.com/services/xml/rss/nyt/Music.xml","https://rss.nytimes.com/services/xml/rss/nyt/Television.xml","https://rss.nytimes.com/services/xml/rss/nyt/Theater.xml"],
    "sports": "https://rss.nytimes.com/services/xml/rss/nyt/Sports.xml",
    "science": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml"
}

TheGuardianResources = {
    "world": "https://www.theguardian.com/world/rss",
    "sports": "https://www.theguardian.com/uk/sport/rss",
    "tech": "https://www.theguardian.com/uk/technology/rss",
    "science": "https://www.theguardian.com/science/rss",
    "entertainment":["https://www.theguardian.com/music/rss","https://www.theguardian.com/us/film/rss","https://www.theguardian.com/games/rss"]
}

queryStringList = ["Bitcoin", "Harry Potter", "Egyptian Pyramids", "Fifa", "Engineering", "Bank Robbery"]


# Function to produce data and send it to Kafka topic
def produceData():
    # Create a Kafka producer to send messages to the Kafka broker 
    producer = KafkaProducer(bootstrap_servers=['kafka:29092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    # Get news articles from different RSS feeds and send to Kafka topic
    articles = GetRssFeed(CNNResources)
    articles += GetRssFeed(NewYorkTimeResources)
    articles += GetRssFeed(ReutersResources)
    articles += GetRssFeed(TheGuardianResources)
    producer.send(topic = "newsarticles", value = articles)

    # Get news articles from RapidAPI using different query strings and send to Kafka topic
    for queryString in queryStringList:
        articles = GetRapidAPIFeed(queryString)
        if(articles):
            producer.send(topic = "newsarticles", value = articles)

    # articles = NYAPIFeed()
    # producer.send(topic = "newsarticles", value = articles)

# Infinite loop to produce data every 24 hours
while(True):
    try:
       produceData()
    except:
        print("except")
    time.sleep(60*60*24)


