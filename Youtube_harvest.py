import os
import pymongo
import mysql.connector as msql
from mysql.connector import Error
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd
import numpy as np
import json
import googleapiclient
from googleapiclient.discovery import build
api_key='AIzaSyAH1t-p7Hv6lKVDX3382hhlsDNHLG6p8gsa'
channel_ids = ['UCduIoIMfD8tT3KoU0-zBRgQ', 'UCueYcgdqos0_PzNOq81zAFg', 'UCuEIBFfvzHXYE8D5ju0EGrg', 'UC8aFE06Cti9OnQcKpl6rDvQ', 'UCaFHlSOg83nCUIHlFMlUhPw', 'UC8uU_wruBMHeeRma49dtZKA', 'UCbfcBATcKj5Qtfxrv8E-ITQ', 'UCW8Ews7tdKKkBT6GdtQaXvQ', 'UC8tgRQ7DOzAbn9L7zDL8mLg', 'UCf6AGqO98eGk11nfazociVQ']  
youtube = build('youtube', 'v3', developerKey=api_key)
all_data = []
def get_channels_name(youtube, channel_ids):
    
      request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(channel_ids))
      response = request.execute()   
      for i in range(len(response['items'])): 
        data = dict(channel_id=response['items'][i]['id'],channel_name = response['items'][i]['snippet']['title'],
            channel_description=response['items'][i]['snippet']['description'],
            channel_views=response['items'][i]['statistics']['viewCount'],
            channel_subscription=response['items'][i]['statistics']['subscriberCount'],
            channel_total_videos=response['items'][i]['statistics']['videoCount'],
            playlist_ids =response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(data)
      return all_data
channel_data=get_channels_name(youtube, channel_ids)
channel_df= pd.DataFrame(channel_data)
playlist_id=channel_df.loc[channel_df['channel_name']=='Parithabangal', 'playlist_ids'].iloc[0]
def get_videos_ids(youtube, playlist_id):
   request = youtube.playlistItems().list(part='contentDetails', playlistId=playlist_id, maxResults=50)
   response = request.execute()
   video_ids=[]
   for i in range(len(response['items'])):
     video_ids.append(response['items'][i]['contentDetails']['videoId'])
   next_page_token=response['nextPageToken']
   more_pages=True
   while more_pages:
    if next_page_token is None:
       more_pages=False
    else:
      request = youtube.playlistItems().list(part='contentDetails', playlistId=playlist_id, maxResults=50, pageToken=next_page_token)
      response = request.execute()
      for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
      next_page_token=response.get('nextPageToken')
   return video_ids
video_ids = get_videos_ids(youtube, playlist_id)
def get_video_details(youtube, video_ids):
 all_video_stats=[]
 for i in range(0, len(video_ids), 50):
  request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=','.join(video_ids[i:i+50]))
  response = request.execute()
  for video in response['items']:
   video_stats=dict(Title=video['snippet']['title'], video_id=video['id'], Published_date=video['snippet']['publishedAt'], Description=video['snippet']['description'],
                     Tags= video['snippet'].get('tags', []), Thumbnails=video['snippet']['thumbnails'], Caption=video['contentDetails']['caption'],Duration=video['contentDetails']['duration'],
                    Views=video['statistics']['viewCount'], Likes=video['statistics']['likeCount'], Comments=video['statistics'].get('commentCount',0))
   all_video_stats.append(video_stats)
 return all_video_stats
video_details=get_video_details(youtube, video_ids)
video_df=pd.DataFrame(video_details)
def get_comments(youtube, video_ids):
    all_comments = []
    for video_id in video_ids:
        request = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            )
        response = request.execute()
        for  comment in response['items']:
          comments=dict(Comments_text=comment['snippet']['topLevelComment']['snippet']['textDisplay'], comments_author=comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                   comment_publishedAt=comment['snippet']['topLevelComment']['snippet']['publishedAt'])
          all_comments.append(comments)
        return all_comments
comments_details = get_comments(youtube, video_ids)
comments_df=pd.DataFrame(comments_details)
client = MongoClient('mongodb+srv://ushasrinivas38:aaaa@cluster0.gwl7v6x.mongodb.net/?retryWrites=true&w=majority', tlsAllowInvalidCertificates=True)
db = client['MyDatabaseDB']
channel_collection = db['channels']
channel_collection.insert_many(channel_df.to_dict(orient='records'))
video_collection = db['videos']
video_collection.insert_many(video_df.to_dict(orient='records'))
comments_collection = db['comments']
comments_collection.insert_many(comments_df.to_dict(orient='records'))
client = MongoClient('mongodb+srv://ushasrinivas38:1234@cluster0.gwl7v6x.mongodb.net/?retryWrites=true&w=majority', tlsAllowInvalidCertificates=True)
db=client['MyDatabaseDB']
collection=db['channels']
item_details = collection.find()
channel_df = DataFrame(item_details)
channel_df['channel_id'] = channel_df['channel_id'].astype(str)
channel_df['channel_name'] = channel_df['channel_name'].astype(str)
channel_df['channel_description'] = channel_df['channel_description'].astype(str)
channel_df['channel_views'] = channel_df['channel_views'].astype(int)
channel_df['channel_subscription'] = channel_df['channel_subscription'].astype(int)
channel_df['channel_total_videos'] = channel_df['channel_total_videos'].astype(int)
channel_df['playlist_ids'] = channel_df['playlist_ids'].astype(str)
collection=db['videos']
item_details = collection.find()
video_df = DataFrame(item_details)
video_df['Title'] = video_df['Title'].astype(str)
video_df['video_id'] = video_df['video_id'].astype(str)
video_df['Published_date'] = pd.to_datetime(video_df['Published_date'])
video_df['Description'] = video_df['Description'].astype(str)
video_df['Tags'] = video_df['Tags'].astype(str)
video_df['Thumbnails'] = video_df['Thumbnails'].astype(str)
video_df['Caption'] = video_df['Caption'].astype(str)
video_df['Duration'] = video_df['Duration'].astype(str)
video_df['Views'] = video_df['Views'].astype(int)
video_df['Likes'] = video_df['Likes'].astype(int)
video_df['Comments'] = video_df['Comments'].astype(int)

collection=db['comments']
item_details = collection.find()
comments_df = DataFrame(item_details)
comments_df=comments_df[['Comments_text', 'comments_author', 'comment_publishedAt']]
comments_df['Comments_text'] = comments_df['Comments_text'].apply(str)
comments_df['comments_author'] = comments_df['comments_author'].apply(str)
comments_df['comment_publishedAt'] = comments_df['comment_publishedAt'].apply(str)

try:
    connection = msql.connect(host='localhost', user='root',  
                        password='aaaa', database='ytubedb')
    if connection.is_connected():
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        
        create_channel_table_query="""CREATE TABLE IF NOT EXISTS channel_data(channel_id varchar(255),
            channel_name varchar(255),
            channel_description varchar(1000),
            channel_views int,
            channel_subscription int,
            channel_total_videos int,
            playlist_ids varchar(255))"""
        create_video_table_query="""CREATE TABLE IF NOT EXISTS video_data(Title varchar(255),
            video_id varchar(255),
            Published_date datetime,
            Description varchar(10000),
            Tags varchar(1000),
            Thumbnails varchar(1000),
            Caption varchar(255),
            Duration varchar(255),
            Views int,
            Likes int,
            Comments int)"""
        create_comments_table_query = """
           CREATE TABLE IF NOT EXISTS comments_data (
           Comments_text VARCHAR(1000),
           comments_author VARCHAR(255),
           comment_publishedAt VARCHAR(255))"""
        cursor.execute(create_channel_table_query)
        cursor.execute(create_video_table_query)
        cursor.execute(create_comments_table_query)
        print("Table is created....")
      
        #with connection.cursor() as cursor:
                    
        #loop through the data frame
        for i,row in channel_df.iterrows():
          channel_id = str(row['channel_id'])

    # select the other columns from the row
          channel_name = row['channel_name']
          channel_description = row['channel_description']
          channel_views = row['channel_views']
          channel_subscription = row['channel_subscription']
          channel_total_videos = row['channel_total_videos']
          playlist_ids = row['playlist_ids']

    # create a tuple of values
          values = (
           channel_id,
           channel_name,
           channel_description,
           channel_views,
           channel_subscription,
           channel_total_videos,
           playlist_ids)
          sql = "INSERT INTO channel_data VALUES (%s, %s, %s, %s, %s, %s, %s)"
          cursor.execute(sql, values)
          print("Channel record inserted")
    
        for i,row in video_df.iterrows():
            #for i, row in video_df.iterrows():
           sql = "INSERT INTO video_data (Title, video_id, Published_date, Description, Tags, Thumbnails, Caption, Duration, Views, Likes, Comments) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
           values = (
             row['Title'],
             row['video_id'],
             row['Published_date'],
             row['Description'],
             row['Tags'],
             row['Thumbnails'],
             row['Caption'],
             row['Duration'],
             row['Views'],
             row['Likes'],
             row['Comments'])
    
           cursor.execute(sql, values)
           print("Video record inserted")

        for i, row in comments_df.iterrows():
           sql = "INSERT INTO comments_data (Comments_text, comments_author, comment_publishedAt) VALUES (%s, %s, %s)"
           values = (
           row['Comments_text'],
           row['comments_author'],
           row['comment_publishedAt'])
    
           cursor.execute(sql, values)
           print("Comment record inserted")
        connection.commit()
except msql.Error as e:
            print("Error while connecting to MySQL", e)
        