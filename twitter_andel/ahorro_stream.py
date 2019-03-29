
import sys
import os
import platform

#Detect if the module is running on Linux or OSX
if platform.system() == 'Darwin':
    sys.path.append("/Users/juanpablo/Documents/deep_dive/virtualEnv/dd_toolkit")
    sys.path.append("/Users/juanpablo/Documents/deep_dive/virtualEnv/alba_socialmedia_TWIT/streams/keys")
    sys.path.append("/Users/juanpablo/Documents/deep_dive/virtualEnv/alba_socialmedia_TWIT/streams/track")
elif platform.system() == 'Linux':
    sys.path.append("/home/ubuntu/Documents/deep_dive/dd_toolkit")
    #sys.path.append("/home/deep_dive_mbp/streams/keys")
    #sys.path.append("/home/deep_dive_mbp/streams/track")


from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
import elasticsearch
from keysLegislation import elastic_keys

from tweepy.streaming import StreamListener

from tweepy import OAuthHandler
from tweepy import Stream

from datetime import datetime
import requests
import certifi
import json

#Import the necessary methods from tweepy library
from terms import search_terms
from twitter_keys import twitter_keys
from TwitterStreamCloudListener import TwitterStreamCloudListener
import logging

# s3 = boto3.client('s3')
# bucket_name = 'insta-stories'
# folder_name = 'ahorro-posts/'


logger = logging.getLogger(__name__)
format_t = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=format_t,level=logging.INFO)

es = Elasticsearch(elastic_keys['endpoint'],
                          http_auth=(elastic_keys['usr'], elastic_keys['pwd']),
                          timeout=30, max_retries=10,retry_on_timeout=True)

def main():
    keys = twitter_keys[0]
    auth = OAuthHandler(keys['ck'],keys['cs'])
    auth.set_access_token(keys['at'],keys['as'])
    
    topics = search_terms

    while(True):
        try:
            l = TwitterStreamCloudListener(es, 'twitter', 'metoo_mx', topics)
            #l.set_database_connection(es)
            stream = Stream(auth, l)

            #stream.filter(track=['ahorro'], async=True)
            
            #logger.info('Tracking ' + str(topics))
            stream.filter(track=topics, stall_warnings=True)
            
            
        except: #Exception as e: 
            print('Failed Connection, Retrying...')
            #print(e)


if __name__ == '__main__':
    main()
