
# coding: utf-8

# In[19]:

import os
import sys
import platform
import locale
import dbDictUpload as db
import elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection
import certifi
from elasticsearch import helpers
from keysLegislation import elastic_keys
import insta_scraper as insta
from insta_listkeys import keys
import insta_dictionaries as idict
from InstagramAPI import InstagramAPI
from tags import tag_posts
import insta_s3 as is3

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


# In[20]:


def location_geopoint(d):
    if "location_lat" in d.keys():
        logger.info("Location: {}".format(d["url"]))
        d["pin"] = {
            "location" : {
                "lat" : d["location_lat"],
                "lon" : d["location_lng"]
                }
            }

    return d


# In[17]:


def main(tema):
    path = 'instagram_stories/'
    logger.info('Starting...')
    if not os.path.exists(path+tema+"_photos"):
        os.makedirs(path+tema+"_photos")
    if not os.path.exists(path+tema+"_videos"):
        os.makedirs(path+tema+"_videos")
    
    logger.info('Init dicts and database connections...')
    
    es = Elasticsearch(elastic_keys['endpoint'],
                          http_auth=(elastic_keys['usr'], elastic_keys['pwd']),
                          timeout=30, max_retries=10,retry_on_timeout=True)
    
    API = InstagramAPI(keys[2]['user_name'],keys[2]['pwd'])
    API.login()
    for topic,tags in tag_posts.items():
        for tag in tags:
            try:
                logger.info('Tag: {}. Getting media...'.format(tag))
                posts_story = insta.get_tag_feed(tag, API,type_ = 'story')
                logger.info('{} posts'.format(len(posts_story)))
                count_uploded_to_elastic, i = 0, 1 
                for media in posts_story:
                    logger.info('{}/{}'.format(i,len(posts_story)))
                    i += 1
                    logger.info('Building entities...')
                    d = idict.media_story_dict(media)
                    d = location_geopoint(d)
                    d['topic'] = topic
                    d['source'] = 'story'
                    #save story in s3
                    logger.info('Saving in s3')
                    bucket = "insta-stories"
                    s3dir = tema + '_photos'
                    #print('='*100)
                    #print(d['url'])
                    #print(s3dir,d['insta_id2'],path)
                    print('='*100)
                    is3.save_story(d['url'],s3dir,d['insta_id2'],path)
                    s3link = "https://s3-us-west-2.amazonaws.com/{}/{}/{}.{}".format(bucket,s3dir,d['insta_id2'],"png")
                    d['s3_photo'] = s3link
                    if 'video_url' in d.keys():
                        s3dir = tema + '_videos'
                        is3.save_story(d['video_url'],s3dir,d['insta_id2'],path)
                        s3link = "https://s3-us-west-2.amazonaws.com/{}/{}/{}.{}".format(bucket,s3dir,d['insta_id2'],"mp4")
                        d['s3_video'] = s3link
                    #upload to elastic
                    logger.info('Uploading...')
                    d['timestamp'] = datetime.now()
                    #print('tattoos','instagram-locations',d,str(d['insta_id2']))
                    uploaded_elastic = db.single_upload_elastic(es,'instagram',tema,d,id_=str(d['insta_id2']))
                    #if uploaded_elastic:
                    #    count_uploded_to_elastic += 1
                logger.info('{}/{} posts uploaded to elastic for {}'.format(count_uploded_to_elastic,len(posts_story),tag))
            except:
                logging.exception('CRITICAL ERROR uploading posts for: {}'.format(tag,str(sys.exc_info()[0])))
    API.logout()
    logger.info("Finished!!")


# In[18]:


if __name__ == "__main__":
    format_t = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(format=format_t,level=logging.INFO)#filename='congressesMain.log')
    main()

