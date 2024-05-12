from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
import pymysql
import mysql.connector 
import pymongo
from datetime import datetime
import re
import uuid

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyB8jAA6wvwt8ov5YJSmrAfpHtcxES8zgyw"
youtube = build('youtube','v3',developerKey=api_key)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

# FUNCTION TO GET VIDEO IDS
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# FUNCTION TO GET VIDEO DETAILS
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
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
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
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

#MongoDB connection

client=pymongo.MongoClient("mongodb+srv://md:saquibkm@cluster0.uzk1oby.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data_harvesting"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details= get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                    "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"


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

    
    first_channel_details=[]
    db =client["Youtube_data_harvesting"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_o},{"_id":0}):
        first_channel_details.append(ch_data["channel_information"])

    df_first_channel_details= pd.DataFrame(first_channel_details)


    for index,row in df_first_channel_details.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
                                            
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            
            cursor.execute(insert_query, values)
            mydb.commit()
            
        except:
            
            you_data= f"Your given Channel Name {channel_name_o} already exists"
            
            return you_data
            

def playlist_table(channel_name_o):
    
    mydb=mysql.connector.connect(host="localhost",
                                user="root",
                                password="admin",
                                database="youtube_db",
                                port="3306")
    cursor = mydb.cursor()

    create_query ='''create table if not exists playlists(Playlist_Id varchar(150) primary key,
                                                            Title varchar(100),
                                                            Channel_Id varchar(150),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_Count int
                                                            )'''
    cursor.execute(create_query)
    mydb.commit()
    
    
    unique_playlist_details =[]
    db =client["Youtube_data_harvesting"]       
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_o},{"_id":0}):
        unique_playlist_details.append(ch_data["playlist_information"])
        
    df_unique_playlist_details= pd.DataFrame(unique_playlist_details[0])
    

    def convert_iso_datetime(iso_datetime):
            return datetime.fromisoformat(iso_datetime.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')

    for index,row in df_unique_playlist_details.iterrows():
        
        published_at = convert_iso_datetime(row['PublishedAt'])
        insert_query='''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)

                                                values(%s,%s,%s,%s,%s,%s)'''

        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                published_at,
                row['Video_Count'])
        
        try:
                
            cursor.execute(insert_query, values)
            mydb.commit()
            print("Row inserted successfully!")
                
        except mysql.connector.IntegrityError as e:
                
            if e.errno == 1062:         
                print(f"Playlist with ID {row['Playlist_Id']} already exists. Skipping insertion.")
            else:       
                print("Error inserting row:", e)


def videos_table(channel_name_o):
    
    mydb=mysql.connector.connect(host="localhost",
                                user="root",
                                password="admin",
                                database="youtube_db",
                                port="3306")
    cursor = mydb.cursor()

    create_query ='''create table if not exists videos(Channel_Name VARCHAR(150),
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

    unique_videos_details=[]
    db =client["Youtube_data_harvesting"]       
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_o},{"_id":0}):
        
        unique_videos_details.append(ch_data["video_information"])

    df_unique_videos_details= pd.DataFrame(unique_videos_details[0])
    
    for index, row in df_unique_videos_details.iterrows():
        
        published_date = datetime.strptime('2024-05-07T08:26:18Z', '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        insert_query = '''INSERT INTO videos (Channel_Name,
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
                                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

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
                row['Caption_Status']
            )

        cursor.execute(insert_query, values)
        mydb.commit()
        
def comments_table(channel_name_o):
    
    mydb=mysql.connector.connect(host="localhost",
                            user="root",
                            password="admin",
                            database="youtube_db",
                            port="3306")
    cursor = mydb.cursor()
    
    create_query ='''create table if not exists comments(Comment_Id VARCHAR(150) PRIMARY KEY,
                            Video_Id VARCHAR(100),
                            Comment_Text TEXT,
                            Comment_Author VARCHAR(150),
                            Comment_Published TIMESTAMP)'''
    cursor.execute(create_query)
    mydb.commit()

    unique_comments_details=[]
    db =client["Youtube_data_harvesting"]       
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_o}, {"_id":0}):
        unique_comments_details.append(ch_data["comment_information"])

    df_unique_comments_details= pd.DataFrame(unique_comments_details[0])
    
    for index,row in df_unique_comments_details.iterrows():
            
        comment_published = datetime.strptime('2024-05-07T13:26:37Z', '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        comment_id = str(uuid.uuid4())
        insert_query='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)
                                                
                                                values(%s,%s,%s,%s,%s)'''
                                                
        values=(comment_id,
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                comment_published)
                
        cursor.execute(insert_query, values)
        mydb.commit()

def tables(one_channel):
    
    you_data=channels_table(one_channel)
    if you_data:
        return you_data
    
    else:
        playlist_table(one_channel)
        videos_table(one_channel)
        comments_table(one_channel)

    return "Tables created successfully"

def show_channels_table():
    ch_list =[]
    db =client["Youtube_data_harvesting"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({}, {"_id":0, "channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df= st.dataframe(ch_list)
    return df

def show_playlists_table():
    pl_list =[]
    db=client["Youtube_data_harvesting"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({}, {"_id":0, "playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    return df1

def show_videos_table():
    vi_list =[]
    db=client["Youtube_data_harvesting"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({}, {"_id":0, "video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)
    return df2

def show_comments_table():
    comm_list =[]
    db=client["Youtube_data_harvesting"]
    coll1=db["channel_details"]
    for comm_data in coll1.find({}, {"_id":0, "comment_information":1}):
        for i in range(len(comm_data["comment_information"])):
            comm_list.append(comm_data["comment_information"][i])
    df3 = st.dataframe(comm_list)
    
    return df3


#streamlit codes

with st.sidebar:
    st.title(":red[[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("ABSTRACT")
    st.caption("PYTHON")
    st.caption("DATA COLLECTION")
    st.caption("API INTEGRATION")
    st.caption("MONGODB")
    st.caption("MYSQL")
    
    
channel_id = st.text_input("Enter the Channel ID")

if st.button("Get and store data"):
    ch_ids=[]
    db =client["Youtube_data_harvesting"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({}, {"_id":0, "channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in ch_ids:
        st.success("Channel Details of channel id already exists")
        
    else:
        insert = channel_details(channel_id)
        st.success(insert)
        
all_channels=[]
db =client["Youtube_data_harvesting"]
coll1=db["channel_details"]
for ch_data in coll1.find({}, {"_id":0, "channel_information":1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])
        
unique_channel= st.selectbox("SELECT THE CHANNEL",all_channels)

    
if st.button("MIGRATE TO SQL"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    
    show_channels_table()

elif show_table=="PLAYLISTS":
    
    show_playlists_table()

elif show_table=="VIDEOS":
    
    show_videos_table()

elif show_table=="COMMENTS":
    
    show_comments_table()


#sql connection

mydb=mysql.connector.connect(host="localhost",
                            user="root",
                            password="admin",
                            database="youtube_db",
                            port="3306")
cursor=mydb.cursor()

question =st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with highest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. Average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    
    quest1 = '''select title as videos,channel_name as channelname from videos'''
    cursor.execute(quest1)
    tb1= cursor.fetchall()
    df = pd.DataFrame(tb1,columns=["video title","channel name"])
    mydb.commit()
    st.write(df)
    
    
elif question=="2. channels with most number of videos":

    quest2 = '''select channel_name as channelname,total_videos as no_videos from channels order by total_videos desc'''
    cursor.execute(quest2)
    tb2= cursor.fetchall()
    df2 = pd.DataFrame(tb2,columns=["channel name","No of videos"])
    mydb.commit()
    st.write(df2)
    
elif question=="3. 10 most viewed videos":

    quest3 = '''select views as views,channel_name as channelname,title as videotitle from videos where 
                views is not null order by views desc limit 10'''
    cursor.execute(quest3)
    tb3= cursor.fetchall()
    df3 = pd.DataFrame(tb3,columns=["views","channel name","videotitle"])
    mydb.commit()
    st.write(df3)
    
elif question=="4. comments in each videos":

    quest4 = '''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(quest4)
    tb4= cursor.fetchall()
    df4 = pd.DataFrame(tb4,columns=["no of comments","videotitle"])
    mydb.commit()
    st.write(df4)   

elif question=="5. Videos with highest likes":

    quest5 = '''select title as videotitle,channel_name as channelname,likes as likecount
                    from videos where likes is not null order by likes desc'''
    cursor.execute(quest5)
    tb5= cursor.fetchall()
    df5 = pd.DataFrame(tb5,columns=["videotitle","channelname","likecount"])
    mydb.commit()
    st.write(df5)
    
elif question=="6. likes of all videos":

    quest6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(quest6)
    tb6= cursor.fetchall()
    df6 = pd.DataFrame(tb6,columns=["likecount","videotitle"])
    mydb.commit()
    st.write(df6)
    
elif question=="7. views of each channel":

    quest7 = '''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(quest7)
    tb7= cursor.fetchall()
    df7 = pd.DataFrame(tb7,columns=["channel name","totalviews"])
    mydb.commit()
    st.write(df7) 
    

elif question=="8. videos published in the year of 2022":

    quest8 = '''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                    where extract(year from published_date)=2022'''
    cursor.execute(quest8)
    tb8= cursor.fetchall()
    df8=pd.DataFrame(tb8,columns=["videotitle","published_date","channelname"])
    mydb.commit()
    st.write(df8)  
    
elif question=="9. Average duration of all videos in each channel":

    quest9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(quest9)
    tb9= cursor.fetchall()
    df9=pd.DataFrame(tb9,columns=["channelname","averageduration"])
    mydb.commit()
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)
    
elif question=="10. videos with highest number of comments":

    quest10 = '''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(quest10)
    tb10= cursor.fetchall()
    df10=pd.DataFrame(tb10,columns=["video title","channel name","comments"])
    mydb.commit() 
    st.write(df10)