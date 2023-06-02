#importing necessary libraries
from googleapiclient.discovery import build
import pandas as pd
import pymongo
import mysql.connector
import pymysql
from datetime import datetime
import streamlit as st
import time
from sqlalchemy import create_engine
import re
from datetime import timedelta

# API key and mentioning build parameter
apikey = "*******"
youtube = build("youtube", "v3", developerKey = apikey)
scrapped_data = {}

# Defining the functions to scrape data from youtube
def channel_stats(youtube,channel_id):
    request = youtube.channels().list(
        part="snippet,statistics, status",
        id = channel_id)
    data = request.execute()
    scrapped_data = dict(Channel_Name = data["items"][0]["snippet"]["title"], 
                         Channel_Id = channel_id,
                         Subscription_Count = int(data["items"][0]["statistics"]['subscriberCount']),
                         Channel_Views = int(data["items"][0]["statistics"]['viewCount']),
                         videoCount = int(data["items"][0]["statistics"]['videoCount']),
                         Channel_Description = data["items"][0]["snippet"]['description'],
                         status = data["items"][0]["status"]["privacyStatus"],
                         Playlist_Id = [],
                         Playlist_detail = [])
    
    request2= youtube.playlists().list(
               part="snippet",
               channelId= channel_id,
               maxResults=50)
    data1 = request2.execute()   

    for i in range (len(data1["items"])):
        scrapped_data["Playlist_Id"].append(data1["items"][i]['id'])
        scrapped_data["Playlist_detail"].append({data1["items"][i]['id'] : data1["items"][i]['snippet']['title']})
        
    next_page = data1.get("nextPageToken")
    more_page = True
    while more_page:
        if next_page == None:
            more_page = False
        else:
            request2= youtube.playlists().list(
               part="snippet",
               channelId= channel_id,
               pageToken = next_page,
               maxResults=50)
            data1 = request2.execute()
            for i in range (len(data1["items"])):
                scrapped_data["Playlist_Id"].append(data1["items"][i]['id'])
                scrapped_data["Playlist_detail"].append({data1["items"][i]['id'] : data1["items"][i]['snippet']['title']})
                
            next_page = data1.get("nextPageToken")
        
    return scrapped_data

def videos_list (youtube, playlist_Id):
    videos = []

    request = youtube.playlistItems().list(
    part="contentDetails",
    playlistId= playlist_Id,
    maxResults=50
    )
    data = request.execute()
    
    for i in range(len(data["items"])):
        videos.append(data["items"][i]["contentDetails"]["videoId"])
    next_page = data.get("nextPageToken")
    more_page = True
    while more_page:
        if next_page == None:
            more_page = False
        else:
            request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_Id,
            maxResults=50,
            pageToken = next_page)
            data = request.execute()
            for i in range(len(data["items"])):
                videos.append(data["items"][i]["contentDetails"]["videoId"])
            next_page = data.get("nextPageToken")
    return videos


def video_stats(youtube, video_ids, playlist_id):
    video_statistics = []
    for i in range (0,len(video_ids), 50):
        
        request = youtube.videos().list(
            part="contentDetails, snippet, statistics",
            id= ','.join(video_ids[i:i+50])
        )
        data = request.execute()
        for i in range(len(data["items"])):
            input_datetime = data["items"][i]['snippet']['publishedAt']
            input_format = "%Y-%m-%dT%H:%M:%SZ"
            output_format = "%Y-%m-%d %H:%M:%S"

            # Convert the input datetime string to a datetime object
            dt = datetime.strptime(input_datetime, input_format)

            # Convert the datetime object to MySQL format string
            changed_datetime = dt.strftime(output_format)

            duration = data["items"][i]['contentDetails']['duration']

            # Extract hours, minutes, and seconds using regular expressions
            matches = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration)
            hours = int(matches.group(1)[:-1]) if matches.group(1) else 0
            minutes = int(matches.group(2)[:-1]) if matches.group(2) else 0
            seconds = int(matches.group(3)[:-1]) if matches.group(3) else 0
            # Calculate the total duration in seconds
            total_seconds = hours * 3600 + minutes * 60 + seconds

            # Create a timedelta object for the duration
            duration_obj = timedelta(seconds=total_seconds)

            # Convert the duration object to a formatted string
            new_duration = str(duration_obj)
                
            video_data = dict(Video_id = data["items"][i]['id'], 
                                  Playlist_id = playlist_id,
                                  Video_name = data["items"][i]['snippet']['title'],
                                  Video_Description = data["items"][i]['snippet']['description'],
                                  Tags = data["items"][i]['snippet'].get("tags"),
                                  PublishedAt = changed_datetime,
                                  View_Count = int(data["items"][i]['statistics']['viewCount']),
                                  Like_Count =  int(data["items"][i]['statistics'].get('likeCount')) if data["items"][i]['statistics'].get('likeCount') != None else data["items"][i]['statistics'].get('likecount'),
                                  Favorite_Count = int(data["items"][i]['statistics']['favoriteCount']),
                                  Comment_Count = int(data["items"][i]['statistics'].get('commentCount')) if data["items"][i]['statistics'].get('commentCount') != None else data["items"][i]['statistics'].get('commentCount'),
                                  Duration = new_duration,
                                  Thumbnail = data["items"][i]['snippet']['thumbnails']['high']['url'],
                                  Caption_Status = data["items"][i]['contentDetails']['caption']
                                 )
            # if some videos don't have comments                  
            if data["items"][i]['statistics'].get('commentCount') is None or data["items"][i]['statistics'].get('commentCount') == '0'or data["items"][i]['statistics'].get('commentCount') == 0 :
                video_data["comments"] = {}
            else:
                video_data["comments"] = comments(youtube, data["items"][i]['id'])

            video_statistics.append(video_data)
    

    return video_statistics

def comments(youtube, Video_id):
    global video_details
    comments = {}
    #video_statistics = video_stats(youtube, Video_id)
    request = youtube.commentThreads().list(
        part= "snippet",        
        maxResults=100,
        videoId= Video_id
        )
    response = request.execute()
    
    for i in range(len(response['items'])):
        input_datetime = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
        input_format = "%Y-%m-%dT%H:%M:%SZ"
        output_format = "%Y-%m-%d %H:%M:%S"
        # Convert the input datetime string to a datetime object
        dt = datetime.strptime(input_datetime, input_format)
        # Convert the datetime object to MySQL format string
        changed_datetime = dt.strftime(output_format)
        
        comments_data = dict(Comment_Id = response['items'][i]['snippet']
                        ['topLevelComment']['id'],
                        Comment_Text = response['items'][i]['snippet']
                        ['topLevelComment']['snippet']['textOriginal'],
                        Comment_Author = response['items'][i]['snippet']
                        ['topLevelComment']['snippet']['authorDisplayName'],
                        comment_published_date = changed_datetime
                            )
        #video_details[val]['comments'][response['items'][i]['snippet']
                        #['topLevelComment']['id']] = comments_data
        comments[response['items'][i]['snippet']
                        ['topLevelComment']['id']] = comments_data
        
    next_page = response.get("nextPageToken")
    more_page = True
    while more_page:
        if next_page == None:
            more_page = False
        else:
            request = youtube.commentThreads().list(
                part="snippet",
                maxResults=100,
                videoId= Video_id,
                pageToken = next_page
            )
            response = request.execute()
            
            for i in range(len(response['items'])):
                input_datetime = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                input_format = "%Y-%m-%dT%H:%M:%SZ"
                output_format = "%Y-%m-%d %H:%M:%S"
                # Convert the input datetime string to a datetime object
                dt = datetime.strptime(input_datetime, input_format)
                # Convert the datetime object to MySQL format string
                changed_datetime = dt.strftime(output_format)
                comments_data = dict(Comment_Id = response['items'][i]['snippet']
                                ['topLevelComment']['id'],
                                Comment_Text = response['items'][i]['snippet']
                                ['topLevelComment']['snippet']['textOriginal'],
                                Comment_Author = response['items'][i]['snippet']
                                ['topLevelComment']['snippet']['authorDisplayName'],
                                comment_published_date = changed_datetime
                                        )
                #video_details[val]['comments'][response['items'][i]['snippet']
                        #['topLevelComment']['id']] = comments_data
                comments[response['items'][i]['snippet']
                        ['topLevelComment']['id']] = comments_data
            next_page = response.get("nextPageToken")

    return comments
# Cacheing the data
@st.cache_data
def scrape_data(_youtube, channel_id):
    scrapped_data = {}
    scrapped_data['Channel_stats']= channel_stats (youtube, channel_id)
    for playlist_id in scrapped_data['Channel_stats']['Playlist_Id']:
        video_ids = videos_list (youtube, playlist_id)
        video_details = video_stats(youtube, video_ids,playlist_id)
        for j in video_details:
            scrapped_data[j["Video_id"]] = j
    return scrapped_data

# Function to upload data to MongoDB
def upload_to_mongoDB(data):
    channel_name = data['Channel_stats']['Channel_Name']
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["youtube_database"]
    # Store data in the "channels" collection
    channels_collection = db[channel_name]
    for i,j in data.items():
        channels_collection.insert_one({i:j})

# Creating tables in SQL
def create_table():
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kishore123",
        database="youtube_database"
    )
    cursor = cnx.cursor()
    create_channel_table_query = """
        CREATE TABLE channel (
            channel_id VARCHAR(255),
            channel_name VARCHAR(255),
            channel_views INT,
            channel_description TEXT,
            channel_status VARCHAR(255),
            subscription_count INT,
            PRIMARY KEY (channel_id))
    """
    cursor.execute(create_channel_table_query)
    
    create_playlist_table_query = '''
                                CREATE TABLE playlist ( 
                                playlist_id VARCHAR(255),
                                channel_id VARCHAR(255),
                                playlist_name VARCHAR(255),
                                PRIMARY KEY (playlist_id),
                                FOREIGN KEY (channel_id) REFERENCES channel(channel_id) 
                                )'''
    cursor.execute(create_playlist_table_query)
    
    
    create_video_table_query = """
        CREATE TABLE videos (
            video_id VARCHAR(255),
            playlist_id VARCHAR(255),
            video_name VARCHAR(255),
            video_description TEXT,
            published_date DATETIME,
            view_count INT,
            like_count INT,
            favorite_count INT,
            comment_count INT,
            duration TIME,
            thumbnail VARCHAR(255),
            caption_status VARCHAR(255),
            PRIMARY KEY (video_id),
            FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id)
        )
    """
    
    cursor.execute(create_video_table_query)
    
    create_comment_table_query = ''' 
                                CREATE TABLE comment (
                                comment_id VARCHAR(255),
                                video_id VARCHAR(255),
                                comment_text TEXT,
                                comment_author VARCHAR(255),
                                comment_published_date DATETIME,
                                PRIMARY KEY (comment_id),
                                FOREIGN KEY (video_id) REFERENCES videos(video_id) )'''
    cursor.execute(create_comment_table_query)
    
    cnx.commit()
    cnx.close()

#Defining functions to insert a data from MongoDB to MYSQL

def insert_channel_stats_sql(channel_name):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    filter={}
    project={
        '_id': 0
    }
    result = client['youtube_database'][channel_name].find(
      filter=filter,
      projection=project
    )
    for i in result:
        for key,pair in i.items():
            if key == "Channel_stats":
                mydb = mysql.connector.connect(
                      host="localhost",
                      user="root",
                      password="kishore123",
                      database = "youtube_database"
                    )

                mycursor = mydb.cursor()
                insert_channel_query = """
                INSERT INTO channel (channel_id, channel_name, channel_views, 
                channel_description, Subscription_Count, channel_status )
                VALUES (%s, %s, %s, %s, %s, %s)
                    """
                mycursor.execute(insert_channel_query, (
                pair["Channel_Id"],
                pair["Channel_Name"],
                pair["Channel_Views"],
                pair["Channel_Description"],
                pair["Subscription_Count"],
                pair["status"]
                    ))
                for j in pair["Playlist_detail"]:
                    for playlist_id,playlist_name in j.items():
                        insert_playlist_query = """
                        INSERT INTO playlist (channel_id, playlist_id, playlist_name)
                        VALUES (%s, %s, %s)
                            """
                        mycursor.execute(insert_playlist_query, (
                        pair["Channel_Id"],
                        playlist_id,
                        playlist_name
                            ))
                        mydb.commit()
    
    mydb.close()

def insert_video_stats_sql(channel_name):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    filter={}
    project={
        '_id': 0
    }
    result = client['youtube_database'][channel_name].find(
      filter=filter,
      projection=project
    )
    for i in result:
        for key,pair in i.items():
            if key != "Channel_stats":
                mydb = mysql.connector.connect(
                      host="localhost",
                      user="root",
                      password="kishore123",
                      database = "youtube_database"
                    )
                
                mycursor = mydb.cursor()
                insert_video_query = """
                INSERT INTO videos (video_id, playlist_id, video_name, video_description, 
                published_date, view_count, like_count, 
                favorite_count, comment_count, duration,
                thumbnail,caption_status )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                mycursor.execute(insert_video_query, (
                pair["Video_id"],
                pair["Playlist_id"],
                pair["Video_name"],
                pair["Video_Description"],
                pair["PublishedAt"],
                pair["View_Count"],
                pair["Like_Count"],
                pair["Favorite_Count"],
                pair["Comment_Count"],
                pair["Duration"],
                pair["Thumbnail"],
                pair["Caption_Status"]   
                    
                    ))
                mydb.commit()
                mydb.close()

def insert_comments_sql(channel_name):
    data = []
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    filter={}
    project={
        '_id': 0
    }
    result = client['youtube_database'][channel_name].find(
      filter=filter,
      projection=project
    )
    for i in result:
        for key,pair in i.items():
            if key!= "Channel_stats":
                for key, pair1 in pair["comments"].items():
                    pair1["video_id"] = pair["Video_id"]
                    data.append(pair1)

    comments_data = pd.DataFrame(data)
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user="root", 
                        pw="kishore123",
                        db="youtube_database"))
    comments_data.to_sql("comment", con = engine, if_exists="append", chunksize = 1000, index = False)

# Creating Streamlit app
   
st.set_page_config(layout="wide")
st.title ("Youtube Data Harvesting")                    
tab1,tab2 = st.tabs(["Scrape", "Analysis"])
with tab1:
        
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["youtube_database"]
    st.write ("Please Enter the Channel Id")
    channel_id = st.text_input('Channel_id')

    if "button1" not in st.session_state:
        st.session_state["button1"] = False

    if "button2" not in st.session_state:
        st.session_state["button2"] = False

    
    if st.button("Scrape"):
        st.session_state["button1"] = not st.session_state["button1"]
        with st.spinner('Wait for it...'):
            time.sleep(5)
            scrapped = scrape_data(youtube, channel_id)
            st.success('Scraping Completed!')

        display = {"Channel Name": scrapped["Channel_stats"]["Channel_Name"],
                    "Channel ID": scrapped["Channel_stats"]["Channel_Id"],
                    "Subscribers Count": scrapped["Channel_stats"]["Subscription_Count"],
                    "Channel Views": scrapped["Channel_stats"]["Channel_Views"],
                    "Video Count": scrapped["Channel_stats"]["videoCount"],
                    "Channel Description": scrapped["Channel_stats"]["Channel_Description"]}
        st.dataframe({"Details":display})
        st.dataframe({"Playlists" :scrapped["Channel_stats"]["Playlist_Id"]}) 

    if st.session_state["button1"]:
        if st.button("Upload to database"):
            st.session_state["button2"] = not st.session_state["button2"]   
            scrapped = scrape_data(youtube, channel_id)
            upload_to_mongoDB(scrapped)
            st.success('Successfully Uploaded to MongoDB')

    option = st.selectbox(
        'Select the Database',db.list_collection_names()
    )

    if st.button("Migrate to SQL"):
        insert_channel_stats_sql(option)
        insert_video_stats_sql(option)
        insert_comments_sql(option)
        st.success('Successfully Migrated to SQL!')
        
# Creating analysis tab in app   
with tab2:
    # Creating questions 
    Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10=("What are the names of all the videos and their corresponding channels?",
        "Which channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of likes for each video, and what are their corresponding video names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year2022?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "Which videos have the highest number of comments, and what are their corresponding channel names?")
    Questions = st.selectbox("Select the Question",
        (Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10)
    )
    # SQL connection
    con = pymysql.connect(host = "localhost",
                      user = "root",
                      password = "kishore123",
                      db = "youtube_database")
    cursor = con.cursor()
    if Questions == Q1:
        channel_name = st.selectbox(
        'Select the Channel Name',db.list_collection_names()
         )
        df = cursor.execute('''SELECT channel.channel_name, videos.video_name
                            from channel
                            left join playlist on playlist.channel_id = channel.channel_id
                            left JOIN videos ON videos.playlist_id = playlist.playlist_id
                            where channel_name = "{}"'''.format(channel_name))
        output = cursor.fetchall()
        title = ["Channel Name","Video Name"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df["Video Name"], width = 1400)
    elif Questions == Q2:
        df = cursor.execute('''SELECT c.channel_id, c.channel_name, COUNT(v.video_id) AS video_count
                                FROM channel c
                                JOIN playlist p ON c.channel_id = p.channel_id
                                JOIN videos v ON p.playlist_id = v.playlist_id
                                GROUP BY c.channel_id, c.channel_name
                                ORDER BY video_count DESC;''')
        output = cursor.fetchall()
        title = ['Channel ID', "Channel Name", "VIdeo Count"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df, width = 1400)
    elif Questions == Q3:
        channel_name = st.selectbox(
        'Select the Channel Name',db.list_collection_names()
         )
        df = cursor.execute('''
        SELECT v.video_name, c.channel_name, v.view_count
        FROM videos v
        JOIN playlist p ON v.playlist_id = p.playlist_id
        JOIN channel c ON p.channel_id = c.channel_id
        WHERE c.channel_name = '{}'
        ORDER BY v.view_count DESC
        LIMIT 10;'''.format(channel_name))
        output = cursor.fetchall()
        title = ["Video Name", "Channel Name", "View Count"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df, width = 1400)
    elif Questions == Q4:
        df = cursor.execute(
            '''
            SELECT v.video_name, COUNT(c.comment_id) AS comment_count
            FROM videos v
            LEFT JOIN comment c ON v.video_id = c.video_id
            GROUP BY v.video_name'''
        )
        output = cursor.fetchall()
        title = ["Video Name", "Comment Count"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df,width = 1400)
    elif Questions == Q5:
        df = cursor.execute('''SELECT v.video_name, c.channel_name, v.like_count
            FROM videos v
            JOIN playlist p ON v.playlist_id = p.playlist_id
            JOIN channel c ON p.channel_id = c.channel_id
            WHERE v.like_count = (SELECT MAX(like_count) FROM videos)
            ORDER BY v.like_count DESC;
            ''')
        output = cursor.fetchall()
        title = ["Video Name", "Channel Name", "Like Count"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df, width = 1400)

        channel_name = st.selectbox(
        'Select the Channel Name',db.list_collection_names()
         )

        df1 = cursor.execute('''SELECT v.video_name, c.channel_name, v.like_count
                    FROM videos v
                    JOIN playlist p ON v.playlist_id = p.playlist_id
                    JOIN channel c ON p.channel_id = c.channel_id
                    WHERE c.channel_name = '{}' AND v.like_count = (SELECT MAX(like_count) FROM videos 
                    WHERE playlist_id IN (SELECT playlist_id FROM playlist 
                    WHERE channel_id = (SELECT channel_id FROM channel WHERE channel_name = '{}')))
                    '''.format(channel_name,channel_name))
        output1 = cursor.fetchall()
        df1 = pd.DataFrame(output1)
        df1.columns = title
        st.dataframe(df1, width = 1400)

    elif Questions == Q6:
        df = cursor.execute('''SELECT v.video_name, SUM(v.like_count) AS total_likes
                                FROM videos v
                                GROUP BY v.video_name;''')
        output = cursor.fetchall()
        title = ["Video Name", "Total Likes"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df, width = 1400)

    elif Questions == Q7:
        df = cursor.execute(
            '''SELECT c.channel_name, SUM(v.view_count) AS total_views
                FROM channel c
                JOIN playlist p ON c.channel_id = p.channel_id
                JOIN videos v ON p.playlist_id = v.playlist_id
                GROUP BY c.channel_name;
                ''')
        output = cursor.fetchall()
        title = ["Channel Name", "Total Views"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df, width = 1400)

    elif Questions == Q8:
        df = cursor.execute(
            '''
            SELECT DISTINCT c.channel_name
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN videos v ON p.playlist_id = v.playlist_id
            WHERE YEAR(v.published_date) = 2022;
            '''
            )
        output = cursor.fetchall()
        title = ["Channel Name"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df, width = 1400)

    elif Questions == Q9:
        df = cursor.execute('''
        SELECT c.channel_name, AVG(TIME_TO_SEC(v.duration)) AS average_duration
        FROM channel c
        JOIN playlist p ON c.channel_id = p.channel_id
        JOIN videos v ON p.playlist_id = v.playlist_id
        GROUP BY c.channel_name;
        ''')
        output = cursor.fetchall()
        title = ["channel Name", "Average Duration"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df, width = 1400)

    elif Questions == Q10:
        st.write(Q10)
        df = cursor.execute(
            '''
            SELECT v.video_name, c.channel_name, COUNT(*) AS comment_count
            FROM videos v
            JOIN comment cm ON v.video_id = cm.video_id
            JOIN playlist p ON v.playlist_id = p.playlist_id
            JOIN channel c ON p.channel_id = c.channel_id
            GROUP BY v.video_id, v.video_name, c.channel_name
            ORDER BY comment_count DESC
            LIMIT 10;
            ''')
        output = cursor.fetchall()
        title = ["Video Name", "Channel Name", "Comment Count"]
        df = pd.DataFrame(output)
        df.columns = title
        st.dataframe(df, width = 1400)

    con.close()
