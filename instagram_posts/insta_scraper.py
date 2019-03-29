from InstagramAPI import InstagramAPI
import json
import requests
from datetime import datetime
import insta_dictionaries as idict
'''
User Info
Extract by User
'''
def get_userid(username):
    '''
    Returns the user_id given the user_name
    No API
    '''
    user_id = json.loads(requests.get(
        "https://www.instagram.com/%s?__a=1" % username).text)['user']['id']
    return user_id

def user_info(username,API):
    '''
    Returns a json with the users info
    keys: user_name, pwd as keys
    API (unofficial)
    '''
    #API = InstagramAPI(keys['user_name'],keys['pwd'])
    #API.login()
    user_id = get_userid(username)
    API.getUsernameInfo(user_id)
    user_api = API.LastJson
    #API.logout()
    if user_api['status'] == 'ok':
        return user_api['user']
    else:
        return user_api['status']

def get_user_feed(username):
    '''
    Returns a list of the last 12 posts from the user
    requires json and requests
    Saved media not included
    '''
    
    user = json.loads(requests.get(
            "https://www.instagram.com/%s?__a=1" % username).text)
    return user['user']['media']['nodes']

def get_following(username,API):
    '''
    Returns the list of the accounts the user follows given a username and keys
    InstagramAPI library
    '''
    #API = InstagramAPI(keys['user_name'],keys['pwd'])
    #API.login()
    user_id  = insta.get_userid(username)
    API.getUsernameInfo(user_id)
    following   = []
    next_max_id = True
    while next_max_id:
        print( next_max_id)
        #first iteration hack
        if next_max_id == True: next_max_id=''
        _ = API.getUserFollowings(user_id,maxid=next_max_id)
        following.extend ( API.LastJson.get('users',[]))
        next_max_id = API.LastJson.get('next_max_id','')
    unique_following = {
        f['pk'] : f
        for f in following
    }
    #API.logout()
    following_ = [v for k,v in unique_following.items()]
    return following_

def get_followers(username,API):
    '''
    Returns a list with the followers given a username and keys
    '''
    #API = InstagramAPI(keys['user_name'],keys['pwd'])
    #API.login()
    user_id  = insta.get_userid(username)
    API.getUsernameInfo(user_id)
    followers   = []
    next_max_id = True
    while next_max_id:
        #print( next_max_id)
        #first iteration hack
        if next_max_id == True: next_max_id=''
        _ = API.getUserFollowers(user_id,maxid=next_max_id)
        followers.extend ( API.LastJson.get('users',[]))
        next_max_id = API.LastJson.get('next_max_id','')
    unique_followers = {
        f['pk'] : f
        for f in followers
    }
    #API.logout()
    followers_ = [v for k,v in unique_followers.items()]
    return followers_
'''
Extract by Tag
'''

def get_tag_feed(tag, API,type_ = 'items'):
    '''
    Returns the posts for ranked items, items or stories given a tag, type_ and keys
    InstagramAPI library
    '''
    #API = InstagramAPI(keys['user_name'],keys['pwd'])
    #API.login()
    API.tagFeed(tag) # get media list by tag
    posts = API.LastJson # last response JSON
    #API.logout()
    if type_ == 'story':
        if "story" in posts.keys():
            return posts['story']['items']
        else:
            return []
    else:
        return posts[type_] # items/ranked items

'''
Media comments
'''
def get_comments(media_id,API,until_date = None,count = 100):
    '''
    Returns a list of comments
    '''
    import time
    has_more_comments = True
    max_id            = ''
    comments          = []
    if count>0:
        #API = InstagramAPI(keys['user_name'],keys['pwd'])
        #API.login()
        while has_more_comments:
            _ = API.getMediaComments(media_id,max_id=max_id)
            #comments' page come from older to newer, lets preserve desc order in full list
            for c in reversed(API.LastJson['comments']):
                comments.append(c)
            has_more_comments = API.LastJson.get('has_more_comments',False)
            #evaluate stop conditions
            if count and len(comments)>=count:
                comments = comments[:count]
                #stop loop
                has_more_comments = False
                print ("stopped by count")
            if until_date:
                older_comment = comments[-1]
                dt=datetime.utcfromtimestamp(older_comment.get('created_at_utc',0))
                print(dt)
                #only check all records if the last is older than stop condition
                if dt<=until_date:
                    #keep comments after until_date
                    print(datetime.utcfromtimestamp(c.get('created_at_utc',0)))
                    comments = [
                        c
                        for c in comments
                        if datetime.utcfromtimestamp(c.get('created_at_utc',0)) < until_date
                    ]
                    #stop loop
                    has_more_comments = False
                    print ("stopped by until_date")
            #next page
            if has_more_comments:
                max_id = API.LastJson.get('next_max_id','')
                time.sleep(2)
        #API.logout()
    return comments

'''
Extract by code
'''
def get_post(code):
    post_dict = None
    post_url = 'https://www.instagram.com/p/%s/?__a=1' % code
    print(post_url)
    post = json.loads(requests.get(post_url).text)
    if 'graphql' in post.keys() and 'shortcode_media' in post['graphql'].keys():
        post = post['graphql']['shortcode_media']
        post_dict = idict.direct_post_dict(post)
    return post_dict
