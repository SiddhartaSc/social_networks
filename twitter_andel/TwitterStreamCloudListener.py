from datetime import datetime
from tweepy.streaming import StreamListener
import json
import sys
import os
import platform
if platform.system() == 'Darwin':
    sys.path.append("/Users/juanpablo/Documents/deep_dive/virtualEnv/dd_toolkit")
    sys.path.append("/Users/juanpablo/Documents/deep_dive/virtualEnv/alba_socialmedia_TWIT/streams/keys")
    sys.path.append("/Users/juanpablo/Documents/deep_dive/virtualEnv/alba_socialmedia_TWIT/streams/track")
elif platform.system() == 'Linux':
    sys.path.append("/home/deep_dive_mbp/dd_toolkit")
    sys.path.append("/home/deep_dive_mbp/streams/keys")
    sys.path.append("/home/deep_dive_mbp/streams/track")

import requests
import dbDictUpload as db
import logging
import pprint
pp = pprint.PrettyPrinter(indent=4)

logger = logging.getLogger(__name__)
format_t = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=format_t,level=logging.INFO)

i=0

import boto3

s3 = boto3.client('s3')
bucket_name = 'twitter-topics'
folder_name = 'me-too/'

def upload_s3(p):
    filename = str(p['id'])+'.json'
    p_json = json.dumps(p)
    f = open(filename,"w")
    f.write(p_json)
    f.close()

    s3.upload_file(filename, bucket_name, folder_name + filename)

    os.remove(filename)

def find_text(dict_twt):
    if 'text' in dict_twt:
        return dict_twt['text']
    if 'full_text' in dict_twt:
        return dict_twt['full_text']
    return ''
    
    
def filter_tweet(tweet, topics):
    relevant = True
    priority = 0
    
    if not tweet['lang']=='es':
        relevant = False
    else:
        
        pp.pprint(tweet['user']['id'])
        
        tweet_txt = ''
        
        if 'retweeted_status' in tweet:
            try:
                #tweet_txt = find_text(tweet['retweeted_status']['quoted_status']['extended_tweet'])
                tweet_txt = find_text(tweet['retweeted_status']['extended_tweet'])
            except:
                #tweet_txt = find_text(tweet['retweeted_status']['quoted_status'])
                tweet_txt = find_text(tweet['retweeted_status'])
        '''
        if 'quoted_status' in tweet:
            try:
                tweet_txt = find_text(tweet['quoted_status']['extended_tweet'])
            except:
                tweet_txt = find_text(tweet['retweeted_status'])
        '''
        if 'extended_tweet' in tweet:
            tweet_txt = find_text(tweet['extended_tweet'])
        
        if not tweet_txt:
            tweet_txt = tweet['text']
        
        for t in topics:
            if t in tweet_txt.lower():
                #print(t)
                priority = priority + 1

        if priority < 1:
            relevant = False
        
        tweet['dd_text'] = tweet_txt
        tweet['dd_relevance'] = priority
        
    return relevant

def process_tweet(data, es, index, kind, topics):
    global i
    try:
        tweet = json.loads(data)
        #pp.pprint(tweet)

        if tweet:
            if filter_tweet(tweet, topics):
                #pp.pprint(tweet)
                db.single_upload_elastic(es, index, kind, tweet ,id_=tweet['id'])
                
                print('Tweet: ', tweet['dd_text'])
                print('Relevance: ', tweet['dd_relevance'])
                print('_'*40)
                
                upload_s3(tweet)
                
                i+=1;
                if i%100 == 0:
                    logger.info(str(i)+' tweets uploaded!')
                    #logger.info('Uploaded!!')
    except Exception as ex:
        #print(data)
        logger.error('Failed Processing tweet: '+ str(ex))

class TwitterStreamCloudListener(StreamListener):
    def __init__(self, es, index, kind, topics=None):
        self.es = es
        self.index = index
        self.kind = kind
        self.topics = topics

    def set_database_connection(self, es):
        self.es = es

    def on_data(self, data):
        try:
            process_tweet(data,self.es, self.index, self.kind, self.topics)
        except Exception as ex:
            print("Couldn't fetch Tweet!!" + str(ex))

        return True

    def on_error(self, status):
        #assert False
        logger.error('Stream error. Restarting in a bit. Status: '+str(status))
    
    def on_exception(self, exception):
           print(exception)
           return
    
