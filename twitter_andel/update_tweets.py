import sys
import os
import platform
#Detect if the module is running on Linux or OSX
if platform.system() == 'Darwin': 
    sys.path.append("/Users/juanpablo/Documents/deep_dive/virtualEnv/dd_toolkit")
elif platform.system() == 'Linux': 
    sys.path.append("/home/deep_dive_mbp/dd_toolkit") #Compute engine test
from datetime import datetime,timedelta
import requests
import elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection
import certifi
from elasticsearch import helpers
import json
from tweepy import OAuthHandler
import tweepy 
import sys
import dbDictUpload as db
from news_keys import elastic_keys
import logging

def get_linked_articles(es_client, days = 2,limit=10000):
    index = 'article_index'
    kind = 'article'
    time_now = datetime.now()+timedelta(days=1)
    time_since = datetime.now()-timedelta(days=days)
    result = []
    
    body = {
      "size": limit,
      "query": {
        "bool":{
            "must": [{"exists" : {"field" : "tw_id" }}],
            "filter" : {"range": {"timestamp": {"gte": time_since,"lte": time_now}}}
        }
        }
    }
    query = es_client.search(index, kind, body = body)

    for entity in query['hits']['hits']:
        entity_source = entity['_source']
        entity_source['_id'] = entity['_id']
        result.append(entity_source)
    return result

def get_partial_tweet_dict(tweet):
    tweet_d = {}
    tweet_d['tw_favorite_count'] = tweet.favorite_count
    tweet_d['tw_retweet_count'] = tweet.retweet_count
    doc = {"doc": tweet_d}
    return doc

def update_linked_tweets(es,api,articles):
    count = 0
    for article in articles:
        try:
            tw_id = article['tw_id']
            tweet = api.get_status(tw_id)
            tweet_d = get_partial_tweet_dict(tweet)
            db.update_document_elastic(es,'social_media','tweet',tweet_d,tweet.id_str)
            id_ = article['_id']
            article.pop('_id',None)
            db.update_document_elastic(es,'article_index','article',tweet_d,id_)
            count += 1
        except:
            print('article Error')
    return count



def main():
    es = Elasticsearch(elastic_keys['endpoint'],
                      http_auth=(elastic_keys['usr'], elastic_keys['pwd']),
                      timeout=30, max_retries=10,retry_on_timeout=True)
    auth = OAuthHandler('tAvBPQSZDuhxmEWEMkWYZ04V9', 'XxBkyrYprgP0UyeUjxRA2bY3VtnkkiBHyO6BRxRsfhfpstc5dJ')
    auth.set_access_token('186105728-Er1XEPTnRlyaI6vz0StHprfjMaAtBX50OJVFCiLb', 'LATzmQcKIv8YirvpFX9JAKNTNXklzaG9N53YKjV7wrYrf')
    api = tweepy.API(auth)

    articles = get_linked_articles(es)
    logger.info('Updating tweet info for {} articles'.format(len(articles)))
    updated = update_linked_tweets(es,api,articles)
    logger.info('Updated {} tweet info'.format(updated))

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    format_t = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(format=format_t,level=logging.INFO)
    main()