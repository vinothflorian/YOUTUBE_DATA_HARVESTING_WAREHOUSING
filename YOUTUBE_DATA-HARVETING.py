from mysql import connector
import streamlit as st
import pandas as pd
import numpy as np
import isodate
import datetime as dt
import googleapiclient.discovery


# DATABASE AND TABLE CREATION

connection = connector.connect(host = 'localhost', user = 'root', password = '1234')
cursor = connection.cursor()
db_cr = 'create database if not exists youtube'
cursor.execute(db_cr)

db = 'use youtube'
cursor.execute(db)

ch_cr = 'create table if not exists channel (channel_id varchar(255), channel_name varchar(255), channel_type varchar(255), channel_view int, channel_description text, channel_status varchar(255))'
cursor.execute(ch_cr)

ch_vid = 'create table if not exists video (channel_id varchar(255), video_id varchar(255), video_name varchar(255), video_description text, published_date datetime, view_count int, like_count int, favorite_count int, comment_count int, duration int, thumbnail varchar(255), caption_status varchar(255))'
cursor.execute(ch_vid)

ch_comments = 'create table if not exists comment (channel_id varchar(255), video_id varchar(255), comment_id varchar(255), comment_text text, comment_author varchar(255), comment_published_date datetime)'
cursor.execute(ch_comments)

#----- END OF DATABASE AND TABLE CREATION --------



# to connect with youtube api

api_service_name = "youtube"
api_version = "v3"
api_key="Enetr your API key"
youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)

tab1, tab2 = st.tabs(["Home Page","Query"])


# function to retrieve channel data

def channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics,status",
        id=channel_id)
    response = request.execute()
    data={'channel_Id':channel_id,
        'channel_name':response['items'][0]['snippet']['title'],
        'Subscriber Count': response['items'][0]['statistics']['subscriberCount'],
        'View Count': response['items'][0]['statistics']['viewCount'],
        'channel_type':response['items'][0]['snippet'].get("categoryId", "Unknown Category"),
        'channel_des':response['items'][0]['snippet']['description'],
        'channel_pub':response['items'][0]['snippet']['publishedAt'],
        'channel_thum':response['items'][0]['snippet']['thumbnails'],
        'channel_status':response['items'][0]['status']['privacyStatus'],
        'Playlist_id':response['items'][0]['contentDetails']['relatedPlaylists']['uploads']}
    return data

# function to fetch the video,video info, comment info
def vidlist(chid):
   video_ids = []
   next_page_token = None

   while True:
        request = youtube.search().list(
            part="snippet",
            channelId= chid,
           
            pageToken=next_page_token,
            type="video"
        )
        response = request.execute()

        # Extract video IDs
        for item in response["items"]:
            if item["id"]["kind"] == "youtube#video":
                video_ids.append(item["id"]["videoId"])

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
   video_info = []  
   comments = []      
   for vid in list(set(video_ids)) :
        request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id = vid)
        response = request.execute()
       
        try:
               data ={ 'Ch_vid_id' : chid,
                'video_id' : vid,
                'video_name' : response['items'][0]['snippet']['title'],
                'video_description' : response['items'][0]['snippet']['description'],
                'published_date' : response['items'][0]['snippet']['publishedAt'],
                'view_count' :  response['items'][0]['statistics']['viewCount'],
                'like_count' :  response['items'][0]['statistics']['likeCount'],
                'favorite_count' :  response['items'][0]['statistics']['favoriteCount'],
                'comment_count' :  response['items'][0]['statistics']['commentCount'],
                'duration' :  isodate.parse_duration(response['items'][0]['contentDetails']['duration']).total_seconds(),
                'thumbnail' : response['items'][0]['snippet']['thumbnails'],
                'caption' :  response['items'][0]['contentDetails']['caption']}
        except KeyError:
            pass
        video_info.append(data)

        next_page_token = None

        while True:
            request = youtube.commentThreads().list(
            part="snippet, replies, id",
            videoId= vid,
            pageToken=next_page_token,
            textFormat='plainText')
            response = request.execute()
            for item in response['items']:
                try:
                    data1= { 'comment_id' : item['id'],
                    'video_id' : item['snippet']['topLevelComment']['snippet']['videoId'],
                    'channel_id' : item['snippet']['topLevelComment']['snippet']['channelId'],   
                    'comment_text' : item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_author' : item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_published_date' : item['snippet']['topLevelComment']['snippet']['publishedAt']}
                    comments.append(data1)
                except KeyError:
                    pass
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
   df_video = pd.DataFrame(video_info)   
   df_comments = pd.DataFrame(comments)   

   return df_video, df_comments

# function to execute select query

def qr_execute(query):
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    return df


# streamlit app

# to display guvi logo

logo_path = r"https://cdn.brandfetch.io/id3BHBKuok/w/400/h/400/theme/dark/icon.jpeg?c=1bx1734702083302id64Mup7ackc0rrClv&t=1727253841190" # Replace with your actual path

# HTML and CSS to position the logo in the bottom-right corner
st.markdown(f"""
    <style>
    .logo {{
        position: fixed;
        bottom: 10px;
        right: 10px;
        width: 40px; 
    }}
    </style>
    <img class="logo" src="{logo_path}" />
""", unsafe_allow_html=True)

with tab1:
    #st.subheader("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.markdown('<h1 style="color: red; font-size: 30px; font-weight: bold;">Youtube Data Harvesting and Warehousing</h1>', unsafe_allow_html=True)
    st.write("This project deals with fetching youtube data, storing it in a database and then getting required information correspeonding to any channel")
    ch_id = st.text_input('Enter channel id:')
    if st.button('Get Info'):
       output= channel_data(ch_id)
       st.image(output['channel_thum']['default']['url'], width = 200, caption = f"{output['channel_name']}")
       st.write(f"**Channel Name        :** {output['channel_name']}")
       st.write(f"**Channel Description :** {output['channel_des']}")
       st.write(f"**Subscriber Count    :** {output['Subscriber Count']}")
       
       
       
    if st.button('Load to DB'):
       
       # insert function - channel dataframe 
       ch_df = pd.DataFrame(channel_data(ch_id))
       ch_df = ch_df.drop(['Subscriber Count', 'channel_pub', 'channel_thum', 'Playlist_id'], axis = 1)
       #ch_df['View Count'] = pd.to_numeric (ch_df['View Count'], downcast = 'integer')
       ch_data = []
       ch_data.append(tuple(ch_df.loc['default']))

       #st.write(ch_data)   

       ch_ck = []
       t = f"select channel_id from channel where channel_id = '{ch_id}' "
       cursor.execute(t)
       for i in cursor:
           ch_ck.append(i)
       if len(ch_ck) < 1:
           ch_in = 'insert into channel (channel_id, channel_name, channel_view, channel_type,  channel_description, channel_status) values (%s, %s, %s, %s, %s, %s)'
           cursor.executemany(ch_in, ch_data)
           connection.commit()
           

           # inserting video and comment data to the database

           video_df, comment_df = vidlist(ch_id)

           video_df.drop(columns= 'thumbnail', inplace=True, axis=1)

           video_df['published_date'] = pd.to_datetime(video_df['published_date'])
           video_df['published_date'] = video_df['published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
           #video_df['view_count'] = pd.to_numeric(video_df['view_count'], downcast='integer')
           #video_df['like_count'] = pd.to_numeric(video_df['like_count'], downcast='integer')
           #video_df['favorite_count'] = pd.to_numeric(video_df['favorite_count'], downcast='integer')
           #video_df['comment_count'] = pd.to_numeric(video_df['comment_count'], downcast='integer')
           #video_df['duration'] = pd.to_numeric(video_df['duration'], downcast='integer')
           video_df.fillna({'Ch_vid_id': 'NA', 'video_id':'NA', 'video_name':'NA', 'video_description': 'NA', 'published_date':'1900-01-01', 'view_count':0, 'like_count':0, 'favorite_count':0, 'comment_count':0, 'duration':0,  'caption':'NA'}, inplace=True)
           
           
           
           vid_info = []

           for vid in video_df.index:
               vid_info.append(tuple(video_df.loc[vid]))
            
           #st.write(vid_info)
           vid_in = 'insert into video (channel_id, video_id, video_name, video_description, published_date, view_count, like_count, favorite_count, comment_count, duration,  caption_status) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
           cursor.executemany(vid_in, vid_info)
           connection.commit()
           #-----------END OF VIDEO INSERT---------------------

           #INSERTING COMMENT INFO

           #st.write(comment_df)
           comment_df['comment_published_date'] = pd.to_datetime(comment_df['comment_published_date'])
           comment_df['comment_published_date'] = comment_df['comment_published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
           comment_df.fillna({'comment_id': 'NA', 'video_id': 'NA', 'channel_id': 'NA', 'comment_text': 'NA', 'comment_author': 'NA', 'comment_published_date': '1900-01-01'}, inplace=True)

           com_info = []
           for com in comment_df.index:
               com_info.append(tuple(comment_df.loc[com]))
            
           #st.write(com_info)
            
           comm_ins = 'insert into comment (comment_id, video_id, channel_id, comment_text, comment_author, comment_published_date) values (%s, %s, %s, %s, %s, %s) '
           cursor.executemany(comm_ins, com_info)
           connection.commit()
           st.write ("<p style='color: green; font-weight: bold;'>Channel info added to DB!</p>", unsafe_allow_html=True)

       else:
           st.write ("<p style='color: red; font-weight: bold;'>Channel Data already available in DB!</p>", unsafe_allow_html=True)

        
       
           
with tab2:
   options = st.selectbox(
    "Select a Query",("None",
    "1.What are the names of all the videos and their corresponding channels?",
    "2.Which channels have the most number of videos, and how many videos dothey have?", 
    "3.What are the top 10 most viewed videos and their respective channels?",
    "4.How many comments were made on each video, and what are their corresponding video names?",
    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
    "8.What are the names of all the channels that have published videos in the year 2022?",
    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

   if st.button("Result", type="primary"):
        if options=="1.What are the names of all the videos and their corresponding channels?":
            query="select DISTINCT A.video_name, b.channel_name  from video A inner join channel B on A.channel_id = B.channel_id "
            st.write(qr_execute(query))
        elif options=="2.Which channels have the most number of videos, and how many videos dothey have?":
            query = "Select * from (select b.channel_name, count(a.video_id) as 'Video Count' from video A  inner join channel B on A.channel_id = B.channel_id group by b.channel_name order by 2 desc) A1"
            st.write(qr_execute(query))
        elif options=="3.What are the top 10 most viewed videos and their respective channels?":
            query = "Select * from (select b.channel_name, a.video_name , a.view_count  from video A  inner join channel B on A.channel_id = B.channel_id order by 3 desc) A1 limit 10"
            st.write(qr_execute(query))
        elif options=="4.How many comments were made on each video, and what are their corresponding video names?":
            query = "select a.video_name,  count(z.comment_id) as 'Comments_count' from video a inner join comment z on a.video_id = z.video_id group by a.video_name order by 2 desc"
            st.write(qr_execute(query))
        elif options=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
            query = "select z.video_name, z.like_count, z.channel_name from (select  A.video_name, a.like_count  , b.channel_name, row_number() over (partition by b.channel_name order by a.like_count desc) as row_no from video A inner join channel B on A.channel_id = B.channel_id order by b.channel_name, a.like_count desc) z where z.row_no = 1 order by z.like_count desc"
            st.write(qr_execute(query))
        elif options=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
            query = "select  A.video_name, a.like_count   from video A inner join channel B on A.channel_id = B.channel_id order by a.like_count desc"
            st.write(qr_execute(query))
        elif options=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
            query = "select channel_name, channel_view from channel"
            st.write(qr_execute(query))
        elif options=="8.What are the names of all the channels that have published videos in the year 2022?":
            query = "select  distinct  b.channel_name  from video A inner join channel B on A.channel_id = B.channel_id where year(a.published_date) = 2022;"
            st.write(qr_execute(query))
        elif options=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            query = "select  a.video_name, avg(a.duration) as 'Average Duration in Seconds',  b.channel_name  from video A inner join channel B on A.channel_id = B.channel_id group by a.video_name, b.channel_name"
            st.write(qr_execute(query))
        elif options=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
            query = """select y.video_name, y.comments_count, y.channel_name from (select z.video_name, z.comments_count, z.channel_name, row_number() over (partition by z.channel_name order by z.comments_count desc) as row_no
                       from (select a.video_name,  count(z.comment_id) as 'Comments_count', 
                     y.channel_name from video a inner join comment z on a.video_id = z.video_id  
                     inner join channel y on a.channel_id = y.channel_id group by a.video_name, y.channel_name order by  2 desc) z) y where y.row_no = 1
                     order by y.comments_count desc"""
            st.write(qr_execute(query))
        else:
            st.write("please select a query from the list")
