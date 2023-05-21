from googleapiclient.discovery import build
import streamlit as st
import pandas as pd


#channel_id = "UCnjU1FHmao9YNfPzE039YTw"
youtube = build("youtube", "v3", developerKey = apikey)
scrapped_data = {}

st.title ("Youtube Data Harvesting")
channel_id = st.text_input('Channel_id', 'Please enter the channel Id')
submit = st.button("scrape")



def channel_stats (youtube,channel_id):
    request = youtube.channels().list(
        part="snippet,statistics",
        id = channel_id)
    data = request.execute()
    scrapped_data = dict(Channel_Name = data["items"][0]["snippet"]["title"], Channel_Id = channel_id,
                                    Subscription_Count = int(data["items"][0]["statistics"]['subscriberCount']),
                                    Channel_Views = int(data["items"][0]["statistics"]['viewCount']),
                                    videoCount = int(data["items"][0]["statistics"]['videoCount']),
                                    Channel_Description = data["items"][0]["snippet"]['description'],
                                    Playlist_Id = [])
    
    request2= youtube.playlists().list(
               part="snippet",
               channelId= channel_id,
               maxResults=50)
    data1 = request2.execute()                         
    for i in range (len(data1["items"])):
        scrapped_data["Playlist_Id"].append(data1["items"][i]['id'])
        #playlist_name.append(data1["items"][i]['snippet']['title'])
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
                #data1["items"][i]['snippet']['title']}
                scrapped_data["Playlist_Id"].append(data1["items"][i]['id'])
               
                #playlist_name.append(data1["items"][i]['snippet']['title'])
     
            next_page = data1.get("nextPageToken")
        
    return scrapped_data

def videos_list (youtube, playlist_Id):
    videos = []
    try:
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
    except:
        pass
    return videos

def video_stats(youtube, video_ids):
    video_statistics = []
    for i in range (0,len(video_ids), 50):
        
        request = youtube.videos().list(
            part="contentDetails, snippet, statistics",
            id= ','.join(video_ids[i:i+50])
        )
        data = request.execute()
        for i in range(len(data["items"])):
            
            video_data = dict(Video_id = data["items"][i]['id'], Video_name = data["items"][i]['snippet']['title'],
                        Video_Description = data["items"][i]['snippet']['description'],
                        Tags = data["items"][i]['snippet'].get("tags"),
                        PublishedAt = data["items"][i]['snippet']['publishedAt'],
                        View_Count = int(data["items"][i]['statistics']['viewCount']),
                        Like_Count = int(data["items"][i]['statistics']['likeCount']),
                        Favorite_Count = int(data["items"][i]['statistics']['favoriteCount']),
                        Comment_Count = data["items"][i]['statistics'].get('commentCount'),
                        Duration = data["items"][i]['contentDetails']['duration'],
                        Thumbnail = data["items"][i]['snippet']['thumbnails']['high']['url'],
                        Caption_Status = data["items"][i]['contentDetails']['caption'],
                        comments = {}
                       )
            video_statistics.append(video_data)
    

    return video_statistics

def comments(youtube, Video_id, val):
    global video_details
    #video_statistics = video_stats(youtube, Video_id)
    request = youtube.commentThreads().list(
        part= "snippet",        
        maxResults=100,
        videoId= Video_id
        )
    response = request.execute()
    for i in range(len(response['items'])):
        comments_data = dict(Comment_Id = response['items'][i]['snippet']
                        ['topLevelComment']['id'],
                        Comment_Text = response['items'][i]['snippet']
                        ['topLevelComment']['snippet']['textOriginal'],
                        Comment_Author = response['items'][i]['snippet']
                        ['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublishedAt = response['items'][i]['snippet']
                        ['topLevelComment']['snippet']['publishedAt']
                                )
        video_details[val]['comments'][response['items'][i]['snippet']
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
                pageToken = next_page)
            response = request.execute()
            for i in range(len(response['items'])):
                comments_data = dict(Comment_Id = response['items'][i]['snippet']
                                ['topLevelComment']['id'],
                                Comment_Text = response['items'][i]['snippet']
                                ['topLevelComment']['snippet']['textOriginal'],
                                Comment_Author = response['items'][i]['snippet']
                                ['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_PublishedAt = response['items'][i]['snippet']
                                ['topLevelComment']['snippet']['publishedAt']
                                        )
                video_details[val]['comments'][response['items'][i]['snippet']
                        ['topLevelComment']['id']] = comments_data
            next_page = response.get("nextPageToken")

    return video_details

if submit == True:
    scrapped_data['Channel_stats']= channel_stats (youtube,channel_id)
    for i in scrapped_data['Channel_stats']['Playlist_Id']:
        video_ids = videos_list (youtube, i)
        video_details = video_stats(youtube, video_ids)
        scrapped_data[i] = video_details
        for i in range(len(video_details)):
            if video_details[i]['Comment_Count'] is None:
                pass
            else:
                video_id = video_details[i]['Video_id']
                print (video_id)
                comments(youtube, video_id, i)
print(scrapped_data)
