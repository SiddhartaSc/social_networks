import sys
import platform
import locale
if platform.system() == 'Darwin':
    sys.path.append("/Users/danae/Documents/deep_dive/dd_toolkit")
    sys.path.append("/Users/danae/Documents/deep_dive/inception")
    locale.setlocale(locale.LC_TIME, 'es_ES')
elif platform.system() == 'Linux':
    locale.setlocale(locale.LC_TIME, 'es_ES.utf8')
    sys.path.append("/home/danae/dd_toolkit")
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
from rata_tags import tag_posts

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def location_geopoint(d):
    if "location_lat" in d.keys():
        d["pin"] = {
        "location" : {
            "lat" : d["location_lat"],
            "lon" : d["location_lng"]
        }
    }
    return d


def main():
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
                posts_items = insta.get_tag_feed(tag, API,type_ = 'items')
                logger.info('{} posts'.format(len(posts_items)))
                count_uploded_to_elastic, i = 0, 1
                for media in posts_items:
                    try:
                        logger.info('{}/{}'.format(i,len(posts_items)))
                        i += 1
                        logger.info('Getting comments...')
                        comments = insta.get_comments(str(media['pk']),API,count = media['comment_count'])
                        logger.info('Building entities...')
                        d = idict.insta_dict(media,comments,'tag')
                        d = location_geopoint(d)
                        if "location_name" in d.keys() and "Mexico" in d["location_name"]:
                            d['topic'] = topic
                            d['source'] = 'hashtag'
                            #upload to elastic
                            logger.info('Uploading...')
                            d['timestamp'] = datetime.now()
                            uploaded_elastic = db.single_upload_elastic(es,'ratatouille-loc','instagram-locations',d,id_=str(d['insta_id']))
                            if uploaded_elastic:
                                count_uploded_to_elastic += 1
                    except:
                        logger.error("Could not analyze post")
                logger.info('{}/{} posts uploaded to elastic for {}'.format(count_uploded_to_elastic,len(posts_items),tag))

            except:
                logging.exception('CRITICAL ERROR uploading posts for: {}'.format(tag,str(sys.exc_info()[0])))
    API.logout()
    logger.info('Finished!')


if __name__ == "__main__":
    format_t = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(format=format_t,level=logging.INFO)#filename='congressesMain.log')
    main()
                
               

