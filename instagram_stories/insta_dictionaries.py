from insta_keys import keys
from datetime import datetime
'''
Returns a dictionary with the new names and values of the post for each case: recent media of an account,
media extracted from tags and media extracted from stories. For the first two cases comments are added to the dictionary
The dictionary returned for each case is ready to upload
'''

def media_user_dict(media):
    media_fields = {
    'id':'insta_id',
    '__typename':'type_name',
    'comments_disabled':'comments_disabled',
    'dimensions':'dimensions',
    'owner':'owner_id',
    'thumbnail_src':'url',
    'is_video':'is_video',
    'code':'code',
    'date':'date',
    'caption':'text',
    'comments':'comments_count',
    'likes':'like_count'
}
    d = {}
    for k, v in media.items():
        if k in media_fields.keys():
            if k in ['owner','comments','likes']:
                i = media_fields[k].index('_')+1
                v = v[media_fields[k][i:]]
            elif k == 'date':
                v = datetime.fromtimestamp(v)
            d[media_fields[k]] = v
    d['insta_type'] = 'post'
    d['insta_source'] = 'user'
    return d

def comment_dict(comment):
    d = {}
    for k,v in comment.items():
        if k not in ['content_type','created_at']:
            if k == 'user':
                for c,u in comment[k].items():
                    if c != 'pk':
                        if 'user' not in c:
                            c = 'user_'+c
                        d[c] = u
            elif k == 'pk':
                d['comment_id'] = v
            elif k == 'created_at_utc':
                d['date'] = datetime.fromtimestamp(v)
            else:
                d[k] = v
    return d

def insta_dict(media,comments,type_):
    if type_ == 'user':
        d = media_user_dict(media)
    elif type_ == 'tag':
        d = media_tag_dict(media)
    c_list = []
    for c in comments:
        c_list.append(comment_dict(c))
    d['comments'] = c_list
    #add user_full_name
    d['user_full_name'] = get_name(d['code'])
    return d

def get_name(code):
    import json
    import requests
    name = None
    post_url = 'https://www.instagram.com/p/%s/?__a=1' % code
    try:
        post = json.loads(requests.get(post_url).text)
        if 'graphql' in post.keys() and 'shortcode_media' in post['graphql'].keys():
            post = post['graphql']['shortcode_media']
            name = post['owner']['full_name']
    except:
        name = None
    return name

def media_tag_dict(media):
    not_media_fields = ['caption_is_edited','client_cache_key','comment_likes_enabled',
     'comment_threading_enabled','has_liked','has_more_comments','max_num_visible_preview_comments',
     'organic_tracking_token','photo_of_you','preview_comments']
    chK = {'taken_at':'date','pk': 'insta_id', 'id': 'insta_id2'}
    caption = ['text','did_report_as_spam','has_translation']
    d = {}
    for k,v in media.items():
        if k not in not_media_fields:
            # parse dates
            if k == 'taken_at':
                v = datetime.fromtimestamp(v) 
            # case 1: change of key name
            if k in chK:
                d[chK[k]] = v
            # case 2 and 3, dictionary subfields
            elif k == 'location' or k == 'user':
                for c,u in media[k].items():
                    if c not in ['friendship_status', 'is_unpublished','is_favorite'] and u != '':
                        if k not in c:
                            c = k+'_'+c
                        d[c.replace('pk','id')] = u
            # case 4 for caption fields
            elif k == 'caption':
                for c,u in media[k].items():
                    if c in caption:
                        d[c] = u
            # case 5: picture url
            elif k == 'image_versions2':
                d['url'] = v['candidates'][0]['url']
            # case 6: keys and values that remain the same
            else:
                d[k] = v
    d['insta_type'] = 'post'
    d['insta_source'] = 'tag'
    return d

def media_story_dict(media):
    not_media_fields = ['caption_is_edited','photo_of_you','organic_tracking_token',
                        'video_dash_manifest','client_cache_key']
    chK = {'taken_at':'date','pk': 'insta_id', 'id': 'insta_id2','caption':'text'}
    d = {}
    for k,v in media.items():
        if k not in not_media_fields:
            # parse dates
            if k == 'taken_at' or k == 'expiring_at':
                v = datetime.fromtimestamp(v) 
            # case 1: change of key name
            if k in chK:
                d[chK[k]] = v
            # case 2 dictionary subfields in user
            elif k == 'user':
                for c,u in media[k].items():
                    if c not in ['friendship_status', 'is_unpublished','is_favorite'] and u != '':
                        if k not in c:
                            c = k+'_'+c
                        d[c.replace('pk','id')] = u
            # case 3 location
            elif k == 'story_locations' and len(v)>0 and 'location' in v[0].keys():
                for c,u in v[0]['location'].items():
                    if u != '':
                        c = 'location_'+c
                        d[c.replace('pk','id')] = u
                d[k] = v
            # case 4: picture url
            elif k == 'image_versions2':
                d['url'] = v['candidates'][0]['url']
            # case 5: video url
            elif k == 'video_versions' and len(v)>0:
                for c,u in v[0].items():
                    d['video_'+c] = u
            # case 6: keys and values that remain the same
            else:
                d[k.replace('caption','text')] = v
    d['insta_type'] = 'story'
    d['insta_source'] = 'tag'
    return d

def media_geo_dict(media):
    fields = {
        'id':'insta_id',
        'created_time':'date'
        }
    same_fields = ['type','users_in_photo','filter','tags']
    d = {}
    for k,v in media.items():
        d['insta_type'] = 'post'
        if k == 'user':
            for c,u in media[k].items():
                if 'user' not in c:
                    c = 'user_'+c
                d[c] = u
        elif k == 'images':
            image = v['low_resolution']
            for m,n in image.items():
                d[m] = n
        elif k in fields.keys():
            d[fields[k]] = v
        elif k == 'likes' or k == 'comments':
            d[k[:-1]+'_count'] = v['count']
        elif k == 'link':
            d['code'] = v.split('/')[-2]
        elif k == 'location':
            for c,u in media[k].items():
                c = c.replace('longitude','lng').replace('latitude','lat')
                if k not in c:
                    c = k+'_'+c
                d[c] = u
        elif k in same_fields:
            d[k] = v
        elif k == 'caption':
            if v is None:
                d['text'] = ''
            else:
                d['text'] = v['text']      
    return d