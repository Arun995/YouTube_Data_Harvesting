import pymongo
from googleapiclient.discovery import build
from pprint import pprint
import mysql.connector
import pandas as pd
import streamlit as st
import psycopg2


#Connecting API Key

def API():
    api_key="AIzaSyAic4osp_0Qtyh0Tk0-uQeLd1ypLQwsypY"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey='AIzaSyAic4osp_0Qtyh0Tk0-uQeLd1ypLQwsypY')
    return youtube
youtube=API()


#Getting channel details

def Channel_det(Channel_ID):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=Channel_ID
    )
    response=request.execute()
    
    for i in response['items']:
        detail=dict(Channel_Id=i['id'],
                Channel_Name=i['snippet']['title'],
                Subscription_Count=i['statistics']['subscriberCount'],
                Channel_Views=i['statistics']['viewCount'],
                Channel_Description=i['snippet']['description'],
                Channel_Video_Count=i['statistics']['videoCount'],
                Channel_Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
               )
    return detail



#Getting video ID

def video_ID(Channel_ID):
    Video_IDs=[]
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=Channel_ID,
    )
    response=request.execute()
    Playlist_ID=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    Page_Token=None
    
    while True:
        req = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=Playlist_ID,
                maxResults=50,
                pageToken=Page_Token
            )
        res = req.execute()
        
        for i in range(len(res['items'])):
            Video_IDs.append(res['items'][i]['snippet']['resourceId']['videoId'])
            Page_Token=res.get('nextPageToken')
            
        if Page_Token==None:
            break
            
    return Video_IDs



#Getting video details


def Video_info(vid_id):
    Video_Det=[]
    for vid in vid_id:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=vid
            )
        response = request.execute()

        for i in response['items']:
            detail=dict(Channel_ID = i['snippet']['channelId'],
                        Channel_Name = i['snippet']['channelTitle'],
                        Video_Title=i['snippet']['title'],
                        Video_ID=i['id'],
                        Video_Description=i['snippet']['description'],
                        Published_Date=i['snippet']['publishedAt'],
                        Thumbnails=i['snippet']['thumbnails']['default']['url'],
                        Video_Tags=i['snippet'].get('tags'),
                        Video_Duration=i['contentDetails']['duration'],
                        Caption_Status=i['contentDetails']['caption'],
                        Definition=i['contentDetails']['definition'],
                        Comment_Count=i['statistics']['commentCount'],
                        Like_Count=i['statistics']['likeCount'],
                        View_Count=i['statistics']['viewCount'],
                        Favorite_Count=i['statistics']['favoriteCount'])
            Video_Det.append(detail)
    return Video_Det


#Getting comment details

def Comment_info(vid_id):
    Comment_det=[]
    try:
        for vid in vid_id:   
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=vid,
                maxResults=50
            )
            response = request.execute()
            for i in response['items']:
                detail=dict(Comment_ID=i['snippet']['topLevelComment']['id'],
                            Video_ID=i['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published_Date=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_det.append(detail)
    except:
        pass
    return Comment_det


#Uploading data to MongoDB

client=pymongo.MongoClient('mongodb://localhost:27017/')
db=client["YouTube_Data"]

def Channel_Info(Channel_ID):
    channel_dets=Channel_det(Channel_ID)
    vid_id=video_ID(Channel_ID)
    video_dets=Video_info(vid_id)
    comment_dets=Comment_info(vid_id)
    
    coll1=db['Channel_Info']
    coll1.insert_one({'Channel_Details':channel_dets,'Video_Details':video_dets,'Comment_Details':comment_dets})
    
    return 'Data Uploaded Successfully'


#Migrate Data from MongoDB & creating tables for channel details


def Channels_Table():

    mydb=psycopg2.connect(host='localhost',
                                 user='postgres', password='test',
                                 database='youtube_data',port='5432')
    mycursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()




    try:
        Create_Query='''create table if not exists channels(Channel_Id varchar(255) primary key,
                                                            Channel_Name varchar(255),
                                                            Subscription_Count bigint,
                                                            Channel_Views bigint,
                                                            Channel_Description text,
                                                            Channel_Video_Count int,
                                                            Channel_Playlist_Id varchar(255))'''
        mycursor.execute(Create_Query)
        mydb.commit()
    except:
        print('Channels table already created')


    ch_list=[]
    db=client['YouTube_Data']
    coll1=db['Channel_Info']
    for ch_data in coll1.find({},{'_id':0,'Channel_Details':1}):
        ch_list.append(ch_data['Channel_Details'])
    df=pd.DataFrame(ch_list)

     #Inserting rows
    for index,row in df.iterrows():
            insert_query='''insert into channels(Channel_Id,
                                                Channel_Name,
                                                Subscription_Count,
                                                Channel_Views,
                                                Channel_Description,
                                                Channel_Video_Count,
                                                Channel_Playlist_Id)

                                                Values(%s,%s,%s,%s,%s,%s,%s)'''
            Values=(row['Channel_Id'],
                    row['Channel_Name'],
                    row['Subscription_Count'],
                    row['Channel_Views'],
                    row['Channel_Description'],
                    row['Channel_Video_Count'],
                    row['Channel_Playlist_Id'])
            try:
                mycursor.execute(insert_query,Values)
                mydb.commit()

            except:
                print('Channels values already inserted')


#Migrate Data from MongoDB & creating tables for video details


def Videos_Table():
    
    mydb=psycopg2.connect(host='localhost',
                                 user='postgres', password='test',
                                 database='youtube_data',port='5432')
    mycursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()


    Create_Query='''create table if not exists videos(Channel_ID varchar(100), 
                                                    Channel_Name varchar(150),
                                                    Video_Title varchar(200),
                                                    Video_ID varchar(200) primary key,
                                                    Video_Description text,
                                                    Published_Date timestamp,
                                                    Thumbnails varchar(200),
                                                    Video_Tags text,
                                                    Video_Duration interval,
                                                    Caption_Status varchar(100),
                                                    Definition varchar(100),
                                                    Comment_Count bigint,
                                                    Like_Count bigint,
                                                    View_Count bigint,
                                                    Favorite_Count int)'''
                       
                                                    
    mycursor.execute(Create_Query)
    mydb.commit()

    vid_list=[]
    db=client['YouTube_Data']
    coll1=db['Channel_Info']
    for vid_data in coll1.find({},{'_id':0,'Video_Details':1}):
        for i in range(len(vid_data['Video_Details'])):
            vid_list.append(vid_data['Video_Details'][i])
    df1=pd.DataFrame(vid_list)

    #Inserting rows
    for index, row in df1.iterrows():
        insert_query = '''
            INSERT INTO videos (
                Channel_ID,Channel_Name, Video_Title, Video_ID, Video_Description, Published_Date, Thumbnails,
                Video_Tags, Video_Duration, Caption_Status, Definition,
                Comment_Count, Like_Count, View_Count, Favorite_Count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
        '''


        values = (
            str(row['Channel_ID']),
            str(row['Channel_Name']),
            str(row['Video_Title']),
            str(row['Video_ID']),
            str(row['Video_Description']),
            str(row['Published_Date']),
            str(row['Thumbnails']),
            str(row['Video_Tags']),
            str(row['Video_Duration']),
            str(row['Caption_Status']),
            str(row['Definition']),
            int(row['Comment_Count']),
            int(row['Like_Count']),
            int(row['View_Count']),
            int(row['Favorite_Count'])
        )

        mycursor.execute(insert_query, values)
        mydb.commit()


#Migrate Data from MongoDB & creating tables for comment details


def Comments_Table():
    
    mydb=psycopg2.connect(host='localhost',
                                 user='postgres', password='test',
                                 database='youtube_data',port='5432')
    mycursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()


    Create_Query='''create table if not exists comments(Comment_ID varchar(200) primary key,
                                                        Video_ID varchar(200),
                                                        Comment_Text text,
                                                        Comment_Author varchar(200),                                                       
                                                        Comment_Published_Date timestamp
                                                        )'''
    mycursor.execute(Create_Query)
    mydb.commit()

    cmt_list=[]
    db=client['YouTube_Data']
    coll1=db['Channel_Info']
    for cmt_data in coll1.find({},{'_id':0,'Comment_Details':1}):
        for i in range(len(cmt_data['Comment_Details'])):
            cmt_list.append(cmt_data['Comment_Details'][i])
    df2=pd.DataFrame(cmt_list)


    #Inserting rows
    for index, row in df2.iterrows():
        insert_query = '''
            INSERT INTO comments (
                Comment_ID, Video_ID, Comment_Text, Comment_Author, Comment_Published_Date
            )
            VALUES (%s, %s, %s, %s, %s)
        '''


        values = (
            str(row['Comment_ID']),
            str(row['Video_ID']),
            str(row['Comment_Text']),
            str(row['Comment_Author']),
            str(row['Comment_Published_Date']),
        )

        mycursor.execute(insert_query, values)
        mydb.commit()

def tables():
    Channels_Table()
    Videos_Table()
    Comments_Table()
    
    return 'Tables are created successfully'
    
def show_ch_table():
    ch_list=[]
    db=client['YouTube_Data']
    coll1=db['Channel_Info']
    for ch_data in coll1.find({},{'_id':0,'Channel_Details':1}):
        ch_list.append(ch_data['Channel_Details'])
    df=st.dataframe(ch_list)
    
    return df


def show_vid_table():
    vid_list=[]
    db=client['YouTube_Data']
    coll1=db['Channel_Info']
    for vid_data in coll1.find({},{'_id':0,'Video_Details':1}):
        for i in range(len(vid_data['Video_Details'])):
            vid_list.append(vid_data['Video_Details'][i])
    df1=st.dataframe(vid_list)
    
    return df1


def show_cmt_table():
    cmt_list=[]
    db=client['YouTube_Data']
    coll1=db['Channel_Info']
    for cmt_data in coll1.find({},{'_id':0,'Comment_Details':1}):
        for i in range(len(cmt_data['Comment_Details'])):
            cmt_list.append(cmt_data['Comment_Details'][i])
    df2=st.dataframe(cmt_list)
    
    return df2

#Streamlit UI

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    
channel_Id = st.text_input("Enter the Channel id")
channels = channel_Id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["YouTube_Data"]
        coll1 = db["Channel_Info"]
        for ch_data in coll1.find({}, {"_id": 0, "Channel_Details": 1}):
            ch_ids.append(ch_data["Channel_Details"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            # Connect to the existing MongoDB database 'YouTube_Data'
            db_existing = client['YouTube_Data']
            coll1_existing = db_existing['Channel_Info']
            
            # Insert data into the existing database
            output = Channel_Info(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_ch_table()
elif show_table ==":red[videos]":
    show_vid_table()
elif show_table == ":blue[comments]":
    show_cmt_table()

#SQL connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="test",
            database= "youtube_data",
            port = "5432"
            )
cursor = mydb.cursor()
    
question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

     
if question == '1. All the videos and the Channel Name':
    query1 = "select video_title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Channel_Video_Count as NO_Videos from channels order by Channel_Video_Count desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select view_count as views , Channel_Name as ChannelName,video_title as VideoTitle from videos 
                        where view_count is not null order by view_count desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select comment_count as No_comments ,video_title as VideoTitle from videos where comment_count is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select video_title as VideoTitle, Channel_Name as ChannelName, like_count as LikesCount from videos 
                       where like_count is not null order by like_count desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select like_count as likeCount,video_title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, channel_views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 = '''select video_title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(video_duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select video_title as VideoTitle, Channel_Name as ChannelName, comment_count as Comments from videos 
                       where comment_count is not null order by comment_count desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
