import shutil
import requests
import boto3
from aws_keys import ACCESS_ID, ACCESS_KEY
import os

def save_local(insta_id,url,type_,path):
    response = requests.get(url, stream=True)
    ext = '.png' if 'photos' in type_ else '.mp4'
    file_name = path+type_+'/'+insta_id+ ext
    with open(file_name, 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)
    del response
    return file_name

def save_s3(path,insta_id,type_,bucket_name = 'insta-stories'):
    s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key=ACCESS_KEY)
    ext = '.png' if 'photos' in type_ else '.mp4'
    file_name = path+type_+'/'+insta_id+ ext
    file_without_path = file_name.split('/')[-1]
    ct = 'image/png' if "photos" in type_ else 'video/mp4'
    return s3.Object(bucket_name, type_+'/'+file_without_path).put(Body=open(file_name, 'rb'),ContentType = ct)

def save_story(url,type_,insta_id,path,bucket_name = 'insta-stories'):
    file_name = save_local(insta_id,url,type_,path)
    save_s3(path,insta_id,type_, bucket_name = bucket_name)
    os.remove(file_name)