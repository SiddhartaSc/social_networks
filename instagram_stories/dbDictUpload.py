from google.cloud import datastore
import elasticsearch
import codecs
from elasticsearch import Elasticsearch, RequestsHttpConnection
import certifi
from decorators import exponential_backoff_retry
import hashlib
from datetime import datetime
import elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection
import certifi
from elasticsearch import helpers

@exponential_backoff_retry
def single_upload_elastic(es,index,doc_type,dict_,id_=None):
    '''
    Uploads a dictonary to elastic cloud
    args: 
        -es: elastic cloud client (Object)
        -index: index namespace :String
        -doc_type: document kind: String
        -dict_: dictionary to upload
        -id_: the unique identifier
    return: void
    '''
    if id_ is None:
        es.index(index=index,doc_type=doc_type, body=dict_)
    else:
        es.index(index=index,doc_type=doc_type,id=id_,body=dict_)
       
@exponential_backoff_retry
def update_document_elastic(es,index,doc_type,dict_,id_):
    es.update(index=index,doc_type=doc_type,id=id_,body=dict_)

def hash_url(url):
    '''
    URL hash
    args: 
        -url:string
    return: hash 
    '''

    return  hashlib.sha1(url.encode()).hexdigest()


@exponential_backoff_retry
def single_upload_gstore(dict_obj,datastore_client,kind,exclude,id_=None):
    '''
    Uploads a dictonary to google datastore

            -id_(optional): the unique identifier, if a URL is given it MUST be hashed before: String
            -dict_obj: the dictonary to store : dict
            -datastore_client: instance of datasrore client (Object)
            -kind: entity kind :String
            -exclude: entity's attribute that are excluded from indexes. Fields longer than. 1500 bytes must be excluded. :list of strings
        returns: datastore entity (Object)
    return: void
    '''
    entity = get_entity_from_dict(dict_obj,datastore_client,kind,exclude,id_)
    datastore_client.put(entity)

def get_entity_from_dict(dict_obj,datastore_client,kind,exclude,id_=None):
    '''
    Builds a google datastore entity from a dictionary
    args: 
        -id_(optional): the unique identifier, if a URL is given it MUST be hashed before: String
        -dict_obj: the dictonary to store : dict
        -datastore_client: instance of datasrore client (Object)
        -kind: entity kind :String
        -exclude: entity's attribute that are excluded from indexes. Fields longer than. 1500 bytes must be excluded. :list of strings
    returns: datastore entity (Object)
    '''
    if id_ is None:
        task_key = datastore_client.key(kind)
    else:
        task_key = datastore_client.key(kind,id_)
        
    d = datastore.Entity(key=task_key, exclude_from_indexes=exclude)
        
    for dict_key in dict_obj.keys():
        d[dict_key] = dict_obj[dict_key]

    return d

def upload_dicts_elastic(dict_list,es,index,doc_type):
    count_elastic=0
    for d in dict_list:
        id_=hash_url(d['url'])
        if(single_upload_elastic(es,index,doc_type,d,id_=id_)):
            count_elastic+=1
    return count_elastic


def upload_dicts(dict_list,datastore_client,kind,es,index,doc_type,exclude=['text']):
    '''
        Given a list of dictionaries uploads them to Google Datastore and Elastic Cloud
        Args: 
        -dic_list: list of dictionaries to upload
        -es: elastic cloud client (Object)
        -index: index namespace :String
        -doc_type: document kind: String
        -dict_obj: the dictonary to store : dict
        -datastore_client: instance of datasrore client (Object)
        -kind: entity kind :String
        Returns: void
    '''
    count_gstore,count_elastic=0,0
    for d in dict_list:
        id_=hash_url(d['url'])

        if(single_upload_gstore(d,datastore_client,kind,exclude,id_=id_)):
            count_gstore+=1

        if(single_upload_elastic(es,index,doc_type,d,id_=id_)):
            count_elastic+=1
    return count_gstore,count_elastic

@exponential_backoff_retry
def update_database_log_news(num_entries,database_kind,newspaper_name,datastore_log_client):
    '''
    Updates log record
    args: 
        -num_entries: Number of entities uploaded
        -kind: Log Kind: String (Ex. newspaper-local-record')
        -database_kind: String (Ex. Elastic or G_Datastore)
        -newspaper_name: String
        -datastore_log_client: instance of datasrore client (Object)
    returns: void
    '''
    kind = 'newspaper-local-record'
    key = datastore_log_client.key(kind)
    d = datastore.Entity(key=key)
    d['database_kind'] = database_kind
    d['date'] = datetime.now()
    d['num_entries'] = num_entries
    d['newspaper_name'] = newspaper_name
    datastore_log_client.put(d)

"""
@exponential_backoff_retry
def update_database_log_news(num_entries,database_kind,newspaper_name,datastore_log_client,kind):
    '''
    Updates log record
    args: 
        -num_entries: Number of entities uploaded
        -kind: Log Kind: String (Ex. newspaper-local-record')
        -database_kind: String (Ex. Elastic or G_Datastore)
        -newspaper_name: String
        -datastore_log_client: instance of datasrore client (Object)
    returns: void
    '''
    
    key = datastore_log_client.key(kind)
    d = datastore.Entity(key=key)
    d['database_kind'] = database_kind
    d['date'] = datetime.now()
    d['num_entries'] = num_entries
    d['newspaper_name'] = newspaper_name
    datastore_log_client.put(d)
"""
@exponential_backoff_retry
def update_database_log(num_entries,database_kind,newspaper_name,datastore_log_client,kind):
    '''
    Updates log record
    args: 
        -num_entries: Number of entities uploaded
        -kind: Log Kind: String (Ex. newspaper-local-record')
        -database_kind: String (Ex. Elastic or G_Datastore)
        -newspaper_name: String
        -datastore_log_client: instance of datasrore client (Object)
    returns: void
    '''
    
    key = datastore_log_client.key(kind)
    d = datastore.Entity(key=key)
    d['database_kind'] = database_kind
    d['date'] = datetime.now()
    d['num_entries'] = num_entries
    d['newspaper_name'] = newspaper_name
    datastore_log_client.put(d)

@exponential_backoff_retry
def update_database_log_elastic_lc(num_entries,datastore_log_client,error_count):
    kind = 'local-congress-record-elastic'
    key = datastore_log_client.key(kind)
    d = datastore.Entity(key=key)
    d['date'] = datetime.now()
    d['num_entries'] = num_entries
    d['error_count'] = error_count
    datastore_log_client.put(d)

@exponential_backoff_retry
def update_database_log_generic(num_entries, kind ,error_count, datastore_log_client):
    kind = kind
    key = datastore_log_client.key(kind)
    d = datastore.Entity(key=key)
    d['date'] = datetime.now()
    d['num_entries'] = num_entries
    d['error_count'] = error_count
    datastore_log_client.put(d)
