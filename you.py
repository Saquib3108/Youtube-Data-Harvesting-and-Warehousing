import pandas as pd
import streamlit as st
import pymongo
import mysql.connector
from googleapiclient.discovery import build
from datetime import datetime
import uuid
import isodate

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyB8jAA6wvwt8ov5YJSmrAfpHtcxES8zgyw"
youtube = build('youtube', 'v3', developerKey=api_key)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response=request.execute()
    for i in response['items']:
        data = dict(Channel_Name=i["snippet"]["title"],
                    Channel_Id=i["id"],
                    Subscribers=i['statistics']['subscriberCount'],
                    Views=i["statistics"]["viewCount"],
                    Total_Videos=i["statistics"]["videoCount"],
                    Channel_Description=i["snippet"]["description"],
                    Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

# FUNCTION TO GET VIDEO IDS
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

# FUNCTION TO GET VIDEO DETAILS
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response["items"]:
            data = dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet'].get('description'),
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

# FUNCTION TO GET COMMENT DETAILS
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()
            for item in response['items']:
                data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except Exception as e:
        print(f"An error occurred: {e}")
    return Comment_data

def get_playlist_details(channel_id): 
    next_page_token=None
    All_data=[]
    while True:
            
            request=youtube.playlists().list(
                    part='snippet,contentDetails',
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
            )
            response=request.execute()

            for item in response['items']:
                
                data=dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Video_Count=item['contentDetails']['itemCount'])
                All_data.append(data)

            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                break
    return All_data

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://md:saquibkm@cluster0.uzk1oby.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["Youtube_data_harvesting"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)
    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information": ch_details, "playlist_information": pl_details,
                      "video_information": vi_details, "comment_information": com_details})
    return "Upload completed successfully"

def channels_table(channel_name_o):
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password="admin",
                                   database="youtube_db",
                                   port="3306")
    cursor = mydb.cursor()
    create_query = '''create table if not exists channels(Channel_Name varchar(150),
                                                        Channel_Id varchar(100) PRIMARY KEY,
                                                        Subscribers BIGINT,
                                                        Views BIGINT,
                                                        Total_Videos int,
                                                        Channel_Description TEXT,
                                                        Playlist_Id varchar(100))'''
    cursor.execute(create_query)
    mydb.commit()
    first_channel_details = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_o}, {"_id": 0}):
        first_channel_details.append(ch_data["channel_information"])
    df_first_channel_details = pd.DataFrame(first_channel_details)
    for index, row in df_first_channel_details.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscribers'],
                  row['Views'],
                  row['Total_Videos'],
                  row['Channel_Description'],
                  row['Playlist_Id'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.IntegrityError:
            return f"Your given Channel Name {channel_name_o} already exists"
    return None

def playlist_table(channel_name_o):
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password="admin",
                                   database="youtube_db",
                                   port="3306")
    cursor = mydb.cursor()
    create_query = '''create table if not exists playlists(Playlist_Id varchar(150) primary key,
                                                            Title varchar(100),
                                                            Channel_Id varchar(150),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_Count int)'''
    cursor.execute(create_query)
    mydb.commit()
    unique_playlist_details = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_o}, {"_id": 0}):
        unique_playlist_details.append(ch_data["playlist_information"])
    df_unique_playlist_details = pd.DataFrame(unique_playlist_details[0])
    def convert_iso_datetime(iso_datetime):
        return datetime.fromisoformat(iso_datetime.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
    for index, row in df_unique_playlist_details.iterrows():
        published_at = convert_iso_datetime(row['PublishedAt'])
        insert_query = '''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                values(%s,%s,%s,%s,%s,%s)'''
        values = (row['Playlist_Id'],
                  row['Title'],
                  row['Channel_Id'],
                  row['Channel_Name'],
                  published_at,
                  row['Video_Count'])
        cursor.execute(insert_query, values)
        mydb.commit()

def videos_table(channel_name_o):
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password="admin",
                                   database="youtube_db",
                                   port="3306")
    cursor = mydb.cursor()
    create_query = '''create table if not exists videos(Channel_Name VARCHAR(150),
                                                        Channel_Id VARCHAR(100),
                                                        Video_Id VARCHAR(50) PRIMARY KEY,
                                                        Title VARCHAR(150),
                                                        Thumbnail VARCHAR(250),
                                                        Description TEXT,
                                                        Published_Date TIMESTAMP,
                                                        Duration VARCHAR(20),
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments INT,
                                                        Favorite_Count INT,
                                                        Definition VARCHAR(25),
                                                        Caption_Status VARCHAR(50))'''
    cursor.execute(create_query)
    mydb.commit()
    unique_videos_details = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_o}, {"_id": 0}):
        unique_videos_details.append(ch_data["video_information"])
    df_unique_videos_details = pd.DataFrame(unique_videos_details[0])
    for index, row in df_unique_videos_details.iterrows():
        published_date = convert_iso_datetime(row['Published_Date'])
        insert_query = '''insert into videos(Channel_Name,
                                             Channel_Id,
                                             Video_Id,
                                             Title,
                                             Thumbnail,
                                             Description,
                                             Published_Date,
                                             Duration,
                                             Views,
                                             Likes,
                                             Comments,
                                             Favorite_Count,
                                             Definition,
                                             Caption_Status)
                                             values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Video_Id'],
                  row['Title'],
                  row['Thumbnail'],
                  row['Description'],
                  published_date,
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_Status'])
        cursor.execute(insert_query, values)
        mydb.commit()

def comments_table(channel_name_o):
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password="admin",
                                   database="youtube_db",
                                   port="3306")
    cursor = mydb.cursor()
    create_query = '''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text TEXT,
                                                        Comment_Author varchar(100),
                                                        Comment_Published TIMESTAMP)'''
    cursor.execute(create_query)
    mydb.commit()
    unique_comments_details = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_o}, {"_id": 0}):
        unique_comments_details.append(ch_data["comment_information"])
    df_unique_comments_details = pd.DataFrame(unique_comments_details[0])
    for index, row in df_unique_comments_details.iterrows():
        comment_published = convert_iso_datetime(row['Comment_Published'])
        insert_query = '''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published)
                                            values(%s,%s,%s,%s,%s)'''
        values = (row['Comment_Id'],
                  row['Video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  comment_published)
        cursor.execute(insert_query, values)
        mydb.commit()

def convert_iso_datetime(iso_datetime):
    return datetime.fromisoformat(iso_datetime.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')

def tables(one_channel):
    you_data = channels_table(one_channel)
    if you_data:
        return you_data
    else:
        playlist_table(one_channel)
        videos_table(one_channel)
        comments_table(one_channel)
        return "Tables created successfully"
    
# Functions to show tables from MongoDB
def show_channels_table():
    ch_list = []
    db = client["Youtube_data_harvesting"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    return df

def show_playlists_table():
    pl_list = []
    db = client["Youtube_data_harvesting"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    return df1

def show_videos_table():
    vi_list = []
    db = client["Youtube_data_harvesting"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    return df2

def show_comments_table():
    comm_list = []
    db = client["Youtube_data_harvesting"]
    coll1 = db["channel_details"]
    for comm_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(comm_data["comment_information"])):
            comm_list.append(comm_data["comment_information"][i])
    df3 = st.dataframe(comm_list)
    return df3

mydb=mysql.connector.connect(host="localhost",
                            user="root",
                            password="admin",
                            database="youtube_db",
                            port="3306")
cursor=mydb.cursor()

def parse_duration(duration):
    # Convert ISO 8601 duration to total seconds
    duration_obj = isodate.parse_duration(duration)
    return duration_obj.total_seconds()

def calculate_avg_duration():
    
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="youtube_db",
        port="3306"
    )
    cursor = mydb.cursor()

    # Ensure durations are stored in seconds for averaging
    cursor.execute("SELECT Channel_Name, Duration FROM videos")
    videos = cursor.fetchall()
    
    duration_data = {}
    for video in videos:
        channel_name, iso_duration = video
        seconds = parse_duration(iso_duration)
        
        if channel_name not in duration_data:
            duration_data[channel_name] = []
        duration_data[channel_name].append(seconds)
    
    avg_duration_data = []
    for channel_name, durations in duration_data.items():
        avg_duration_seconds = sum(durations) / len(durations)
        avg_duration = str(isodate.duration.Duration(seconds=avg_duration_seconds))
        avg_duration_data.append((channel_name, avg_duration))
    
    df_avg_duration = pd.DataFrame(avg_duration_data, columns=["Channel Name", "Average Duration"])
    st.write(df_avg_duration)

def show_avg_duration():
    if st.button("Show Average Duration of All Videos in Each Channel"):
        calculate_avg_duration()
        
        
# Streamlit App

st.set_page_config(page_title="YouTube Data Harvesting", layout="wide", initial_sidebar_state="expanded")

# Sidebar
with st.sidebar:
    st.title(":red_circle: **YouTube Data Harvesting and Warehousing**")
    st.markdown("---")
    st.header("Data Collection")
    st.markdown(
        """
        - **Language:** Python
        - **Tech Stack:** Data Collection, API Integration, MongoDB, MySQL
        """
    )
    st.markdown("---")

st.title("YouTube Data Harvesting and Warehousing")

menu = ["Add Channel", "Show MongoDB Data", "Migrate to SQL", "SQL Queries"]
choice = st.sidebar.selectbox("Main Menu", menu)

if choice == "Add Channel":
    st.subheader("Add a YouTube Channel")
    channel_id = st.text_input("Enter Channel ID:")
    if st.button("Fetch and Store Data"):
        ch_info = get_channel_info(channel_id)
        video_ids = get_videos_ids(channel_id)
        video_data = get_video_info(video_ids)
        comment_data = get_comment_info(video_ids)
        playlist_data = get_playlist_details(channel_id)

        db.channel_details.insert_one({"channel_information": ch_info, "playlist_information": playlist_data, "video_information": video_data, "comment_information": comment_data})
        st.success("Channel Data Added to MongoDB")

elif choice == "Show MongoDB Data":
    st.subheader("Show Data from MongoDB")
    data_option = st.radio("Select Table to Show", ["Channels", "Playlists", "Videos", "Comments"])
    if data_option == "Channels":
        show_channels_table()
    elif data_option == "Playlists":
        show_playlists_table()
    elif data_option == "Videos":
        show_videos_table()
    elif data_option == "Comments":
        show_comments_table()

elif choice == "Migrate to SQL":
    st.subheader("Migrate Data from MongoDB to SQL")
    channel_names = show_channels_table()
    channel_name = st.text_input("Enter Channel Name to Migrate:")
    if st.button("Migrate to SQL"):
        result = tables(channel_name)
        st.success(result)

elif choice == "SQL Queries":
    st.subheader("Run SQL Queries")
    question = st.selectbox("Select Query", [
        "1. Names of all videos and their corresponding channels",
        "2. Channel with the most videos",
        "3. Top 10 most viewed videos and their channels",
        "4. Number of comments per video",
        "5. Channels with the highest number of views",
        "6. Videos with more than 10000 likes",
        "7. views of each channel",
        "8. Channels with the most number of videos",
        "9. Average duration of all videos in each channel",
        "10. Videos with the highest number of comments"
    ])

    if question == "1. Names of all videos and their corresponding channels":
        query = '''SELECT title AS video_title, channel_name FROM videos'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["Video Title", "Channel Name"])
        st.write(df)

    elif question == "2. Channel with the most videos":
        query = '''SELECT channel_name, COUNT(*) AS video_count FROM videos GROUP BY channel_name ORDER BY video_count DESC LIMIT 1'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["Channel Name", "Video Count"])
        st.write(df)

    elif question == "3. Top 10 most viewed videos and their channels":
        query = '''SELECT title AS video_title, channel_name, views FROM videos ORDER BY views DESC LIMIT 10'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["Video Title", "Channel Name", "Views"])
        st.write(df)

    elif question == "4. Number of comments per video":
        query = '''SELECT title AS video_title, comments FROM videos'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["Video Title", "Comments"])
        st.write(df)

    elif question == "5. Channels with the highest number of views":
        query = '''SELECT channel_name, SUM(views) AS total_views FROM videos GROUP BY channel_name ORDER BY total_views DESC'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["Channel Name", "Total Views"])
        st.write(df)

    elif question == "6. Videos with more than 10000 likes":
        query = '''SELECT title AS video_title, likes FROM videos WHERE likes > 10000'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["Video Title", "Likes"])
        st.write(df)

    elif question == "7. views of each channel":
        query = '''select channel_name as channelname ,views as totalviews from channels'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["channel name","totalviews"])
        st.write(df)

    elif question == "8. Channels with the most number of videos":
        query = '''SELECT channel_name, COUNT(*) AS video_count FROM videos GROUP BY channel_name ORDER BY video_count DESC'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["Channel Name", "Video Count"])
        st.write(df)

    elif question == "9. Average duration of all videos in each channel":
        show_avg_duration()
        query = '''SELECT channel_name, AVG(duration) AS avg_duration FROM videos GROUP BY channel_name'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["Channel Name", "Average Duration"])
        st.write(df)

    elif question == "10. Videos with the highest number of comments":
        query = '''SELECT title AS video_title, comments FROM videos ORDER BY comments DESC'''
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["Video Title", "Comments"])
        st.write(df)

# Close connections
cursor.close()
mydb.close()
client.close()