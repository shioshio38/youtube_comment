#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from dotenv import load_dotenv
import os
import googleapiclient.discovery
import datetime
from zoneinfo import ZoneInfo
import logging
from logging import getLogger
from mog_op import MongoOp
logging.basicConfig(level=logging.INFO)
logger = getLogger(__name__)
import googleapiclient.errors
from pprint import pprint
import argparse
import csv
import re
parser = argparse.ArgumentParser(
    prog='get youtube comment',
    description='get youtube comment from channel and related video info and save mongo')
parser.add_argument('--use_cache',action='store_true',help='use cache and reduce use youtube api becase youtube searrch is costly api')
args = parser.parse_args()

logging.basicConfig(level=logging.INFO)
load_dotenv('.env')
DEVELOPER_KEY=os.environ['DEVELOPER_KEY']
YOUTUBE_API_SERVICE_NAME = os.environ['YOUTUBE_API_SERVICE_NAME']
YOUTUBE_API_VERSION = os.environ['YOUTUBE_API_VERSION']
CH_INFO={
    #'CHANNEL_ID':'UCuuVLJljOCZqCie0k0Z6aRg', #岸田 https://m.youtube.com/channel/UCuuVLJljOCZqCie0k0Z6aRg
    #'CH_NAME':'岸田',
    'CHANNEL_ID':'UCidQ51J5ysCWeGBnLuNvnhQ', #石丸 https://m.youtube.com/channel/UCidQ51J5ysCWeGBnLuNvnhQ
    'CH_NAME': "石丸",
    #'CHANNEL_ID':'UCy7aH7KKlIpjX-9aHFfFRSA', #小池 https://m.youtube.com/channel/UCy7aH7KKlIpjX-9aHFfFRSA
    #'CH_NAME': "小池",
    #'CHANNEL_ID':'UCeigzVkpXmZ_t79PBDjh4fQ', #蓮舫 https://m.youtube.com/channel/UCeigzVkpXmZ_t79PBDjh4fQ
    #'CH_NAME': "蓮舫",
}


jst = ZoneInfo("Asia/Tokyo")

def get_authenticated_service():
    return googleapiclient.discovery.build(
        YOUTUBE_API_SERVICE_NAME,YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY
    )

def conv_time(dt):
    return dt.isoformat()

def get_video_list(youtube,nextPageToken):
    before=conv_time(datetime.datetime(2024,7,7,tzinfo=jst))
    after=conv_time(datetime.datetime(2024,6,1,tzinfo=jst))
    logging.info(f"before={before} after={after}")
    response = youtube.search().list(
        part = "snippet",
        channelId = CH_INFO['CHANNEL_ID'],
        maxResults = 50,# 最大取得数
        order = "date", #日付順にソート
        publishedAfter = after, # いつ以降の取得を取得するか
        publishedBefore = before,
        type="video",
        regionCode='jp',
        pageToken = nextPageToken
    ).execute()
    for r in response:
        if r=='items':
            sz=len(response['items'])
            logging.info(f"r={r} sz={sz}")
        else:
            logging.info(f"r={r} response={response[r]}")
    items=response['items']
    nextPageToken = response.get('nextPageToken',None)
    return items,nextPageToken

def get_vid(items):
    return [kk['id']['videoId'] for kk in items]

class VideoInfo(object):
    def __init__(self,i):
        self.vinfo=i
        self.vid=i['id']
        self.title=i['snippet']['title']
        self.publishedAt=i['snippet']['publishedAt']
        st=i['statistics']
        vc=st['viewCount']
        self.vc = int(vc)
        cm=st.get('commentCount',None)
        if cm:
            self.cm=int(cm)
        else:
            self.cm=0 #意味的にはNoneだが後段のsortedでNoneだとsort出来ない。
    def __str__(self):
        return f"{self.vid},{self.publishedAt},viewCount={self.vc},commentCount={self.cm} {self.title}"

def chunk_list(lst, chunk_size):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
    
def get_video_infos(vids,youtube,vinfos,mp):
    if args.use_cache:
        items=[]
        col=mp.get_col('video_info')
        for item in col.find({'id':{"$in":vids}}):
            items.append(item)
    else:
        items=[]
        part='snippet,statistics'
        logger.info(f"vids={vids}")
        v_chunk=chunk_list(vids,50)
        for vids0 in v_chunk:
            vsize=len(vids0)
            vids0=','.join(vids0)
            logger.info(f"vids0={vids0}")
            video_info = youtube.videos().list(part=part, id=vids0).execute()
            items0=video_info['items']
            size=len(items0)
            logger.info(f"vsize={vsize} size={size}")            
            assert vsize==size

            col=mp.get_col('video_info')
            for item in items0:
                items.append(item)
                vid=item['id']
                if not col.find_one({'id':vid}):
                    ch_id=CH_INFO['CHANNEL_ID']
                    item['channel_id']=ch_id
                    col.insert_one(item)
    
    for i in items:
        vi=VideoInfo(i)
        vinfos.append(vi)
        
def get_vlist(youtube,vitems,mp):
    if args.use_cache:
        col=mp.get_col('channel_videos')
        ch_id=CH_INFO['CHANNEL_ID']
        vinfos = col.find_one({'channel_id':ch_id})
        items=vinfos['items']
        for i in items:
            vitems.append(i)
    else:
        items,nextPageToken = get_video_list(youtube,'')
        for i in items:
            vitems.append(i)
        while nextPageToken:
                items,nextPageToken=get_video_list(youtube,nextPageToken)
                for i in items:
                    vitems.append(i)
        ch_id=CH_INFO['CHANNEL_ID']
        ch_name=CH_INFO['CH_NAME']
        ch_info={'channel_id':ch_id,'channel_name':ch_name,'items':vitems}
        col=mp.get_col('channel_videos')
        logger.info(f"col={col}")
        if not col.find_one({'channel_id':ch_id}):
            col.insert_one(ch_info)
        else:
            dkt={"$set":{'channel_name':ch_name,'items':vitems}}
            col.update_one({"channel_id":ch_id},dkt)

            
def get_comments_inner(youtube,vid,pageToken):
    response = youtube.commentThreads().list(
        part='id,snippet',
        videoId=vid,
        maxResults=100,
        order='time',
        pageToken=pageToken,
    ).execute()
    for r in response:
        if r=='items':
            sz=len(response['items'])
            logging.info(f"r={r} sz={sz}")
        else:
            logging.info(f"r={r} response={response[r]}")
    items=response['items']
    
    nextPageToken = response.get('nextPageToken',None)
    return items,nextPageToken
def show_comment(result):
    for r in result[:1]:
        pprint(r)
    
    
def get_comments(youtube,mp,vi,comments,cnt0):
    if args.use_cache:
        col=mp.get_col('comment')
        video_id=vi.vid
        v_title=vi.title
        if re.search("[Mm]eet-up",v_title):
            logger.info(f"match v_title={v_title}")
        else:
            logger.info(f'\tnot match v_title={v_title}')
            return
        for c in col.find({'video_id':video_id}):
            text=c['snippet']['topLevelComment']['snippet']['textOriginal']
            #text=text.replace('\n','')
            text = ''.join(text.split())
            comments.append([text,])
        
    else:
        result=[]
        video_id = vi.vid
        try:
            items,nextPageToken=get_comments_inner(youtube,video_id,None)
            for item in items:
                result.append(item)
            cnt=0
            while nextPageToken:
                items,nextPageToken=get_comments_inner(youtube,video_id,nextPageToken)
                result.append(item)
                cnt+=1
                cnt0[0]+=1
                logger.info(f"cnt={cnt} cnt0={cnt0[0]}")
            col=mp.get_col('comment')
            for r in result:
                cid=r['id']
                if not col.find_one({'id':cid}):
                    r['video_id']=video_id
                    r['channel_id']=CH_INFO['CHANNEL_ID']
                    r['channel_name']=CH_INFO['CH_NAME']
                    col.insert_one(r)
        except googleapiclient.errors.HttpError as err:
            logger.error(f"video_id={video_id} err={err}")
        
def save_csv(name,comments):
    comments=[[c] for c in comments]
    with open(f"{name}.csv",'w',encoding='utf-8') as csvout:
        writer = csv.writer(csvout)
        writer.writerow(['text'])
        writer.writerows(comments)

def main():
    mp = MongoOp(db='youtube_comment',logger=logger)    
    youtube = get_authenticated_service()
    vitems=[]
    get_vlist(youtube,vitems,mp)
    vids=get_vid(vitems)
    video_infos=[]
    get_video_infos(vids,youtube,video_infos,mp)
    comments=[]
    cnt0=[0]
    for vi in video_infos:
        get_comments(youtube,mp,vi,comments,cnt0)

    save_csv(CH_INFO['CH_NAME'],comments)
    mp.close()
if __name__=='__main__':main()
