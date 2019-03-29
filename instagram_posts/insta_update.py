
# coding: utf-8
import sys
import locale
import platform
if platform.system() == 'Darwin': 
    sys.path.append("/Users/danae/Documents/deep_dive/dd_toolkit")
    sys.path.append("/Users/danae/Documents/deep_dive/inception")
    locale.setlocale(locale.LC_TIME, 'es_ES')
elif platform.system() == 'Linux': 
    locale.setlocale(locale.LC_TIME, 'es_ES.utf8')
    sys.path.append("/home/danae/dd_toolkit")
from keysLegislation import elastic_keys
import elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection
import certifi
from elasticsearch import helpers
from datetime import datetime, timedelta
import insta_dictionaries as idict
import dbDictUpload as db
import json
import requests
import logging
logger = logging.getLogger(__name__)

def new_comments_likes(code):
    post_url = 'https://www.instagram.com/p/%s/?__a=1' % code
    post = json.loads(requests.get(post_url).text)
    if 'graphql' in post.keys() and 'shortcode_media' in post['graphql'].keys():
        post = post['graphql']['shortcode_media']
        likes = post['edge_media_preview_like']
        comments = post['edge_media_to_comment']
    return comments,likes

def get_posts(es,index,kind,since,until, limit = 10000,source = "hashtag"):
    # get posts 
    body = {
          "size": limit,
          "query": {
            "bool": { 
              "must":{"match":{"source":source}},      
              "filter" : {"range": {"date": {"gte": since,"lte": until}}}
            }
          },
           "sort": { "date": { "order": "desc" }}
        }
    query = es.search(index, kind, body = body)
    posts = []
    for entity in query['hits']['hits']:
        entity['_source']['_id'] = entity['_id']
        entity_source = entity['_source']
        posts.append(entity_source)
    return posts

def main():
    es = Elasticsearch(elastic_keys['endpoint'],
                          http_auth=(elastic_keys['usr'], elastic_keys['pwd']),
                          timeout=30, max_retries=10,retry_on_timeout=True)
    index = 'ratatouille-loc' 
    kind = 'instagram-locations'
    since = datetime.now() - timedelta (days = 1)
    until = datetime.now()
    logger.info('Getting posts ...')
    posts = get_posts(es,index,kind,since,until)
    logger.info('{} posts to update'.format(len(posts)))
    i = 1
    count_updated = 0
    for post in posts:
        logger.info('{}/{}'.format(i,len(posts)))
        i += 1
        partial_dict = {}
        comments, likes = new_comments_likes(post['code'])
        #falta ver lo de page info cuando hay más
        #comments
        partial_dict['comments'] = [idict.comment_dict(comment['node']) for comment in comments ['edges']]
        partial_dict['comment_count'] = comments['count']
        #likes
        partial_dict['like_count'] = likes['count']
        partial_dict['likers'] = [like['node'] for like in likes['edges']]
        #update
        doc = {"doc": partial_dict}
        if db.update_document_elastic(es,index,kind,doc,post['_id']):
            count_updated += 1
        else:
            logger.error('Could not upload post {} code: {}'.format(post['_id'],post['code']))
    logger.info('{}/{} posts updated'.format(count_updated,len(posts)))
    logger.info("Finished!!")

if __name__ == "__main__":
    format_t = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(format=format_t,level=logging.INFO)#filename='congressesMain.log')
    main()