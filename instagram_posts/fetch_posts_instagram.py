
# coding: utf-8

# In[1]:


'''
Gets posts of instagram related to savings and uploads them to S3
'''
import sys
import platform
import locale
if platform.system() == 'Linux':
    sys.path.append("/home/ubuntu/Documents/deep_dive/dd_toolkit")

import boto3
import json
import os
s3 = boto3.client('s3')

import insta_scraper as insta
from insta_listkeys import keys
import insta_dictionaries as idict
from InstagramAPI import InstagramAPI
#from tags import tag_posts

import logging
import time
from datetime import datetime

import reverse_geocoder as rg

logger = logging.getLogger(__name__)


# In[2]:


def upload_s3(posts, bucket_name, folder_name):
    for p in posts:
        filename = str(p['caption']['media_id'])+'.json'
        p_json = json.dumps(p)
        f = open(filename,"w")
        f.write(p_json)
        f.close()
        
        s3.upload_file(filename, bucket_name, folder_name + filename)
        
        os.remove(filename)


# In[3]:


def filter_location(posts):
    f =[]

    for p in posts:
        if "location" in p.keys():
            lat = p["location"]["lat"]
            lng = p["location"]["lng"]
            
            res = rg.get((lat, lng))
            
            if res['cc'] == 'MX':
                f.append(p)
            
    return f


# In[4]:


def get_posts(API, tag):
    try:
        logger.info('Tag: {}. Getting media...'.format(tag))
        posts_items = insta.get_tag_feed(tag, API, type_ = 'items')
        logger.info('{} posts'.format(len(posts_items)))
        count_uploded_to_elastic, i = 0, 1

        filtered = posts_items
        #filtered = filter_location(posts_items)

        if len(filtered) > 0:
            logger.info('{} from Mexico'.format(len(filtered)))
        else:
            logger.info('0 from Mexico')

    except:
        logging.exception('CRITICAL ERROR uploading posts for: {}'.format(tag,str(sys.exc_info()[0])))
    
    return filtered


# In[5]:


def main(tema):
    logger.info('Init dicts and database connections...')

    bucket_name = 'dd-social-networks'
    folder_name = tema+'/instagram/posts/'
    tags = ['tatuaje', 'tatuajes', 'tattoo', 'tattoos']
    
    API = InstagramAPI(keys[3]['user_name'],keys[3]['pwd'])
    API.login()
    
    for index,tag in enumerate(tags):
        try:
            posts = get_posts(API, tag)
            upload_s3(posts, bucket_name, folder_name)
            print(index,end="")
        except Exception as e:
            print('Error in tag: ', tag)
            print(e)
    
    API.logout()
    logger.info('Finished!')


# In[ ]:


if __name__ == "__main__":
    format_t = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(format=format_t,level=logging.INFO)#filename='congressesMain.log')
    while True:
        main("tattoos")
        time.sleep(100)

