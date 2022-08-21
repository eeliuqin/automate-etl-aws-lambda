import pandas as pd
import requests
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
import pymysql
import os

def get_video_info(content):
    if "video" in content:
        video = content["video"]
        # remove thumbnails
        video.pop("thumbnails", "No Key Found")
        return video
    else:
        return None

def extract_data():
    """
    Type of trending videos:
    n - now (default)
    mu - music
    mo - movies
    g - gaming
    """
    querystring = {"type":"n","hl":"en","gl":"US"}
    url = "https://youtube-search-and-download.p.rapidapi.com/trending"
    # get rapid api key from Environment variables
    headers = {
        "X-RapidAPI-Key": os.environ["rapid_api_key"],
        "X-RapidAPI-Host": "youtube-search-and-download.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    # convert response string to JSON
    response_json = response.json()  
    contents = response_json['contents']
    df = pd.DataFrame([get_video_info(content) for content in contents])
    return df

def get_length_minutes(s):
    t = 0
    for u in s.split(':'):
        t = 60 * t + int(u)
    # convert total seconds to minutes
    return round(t/60, 2)
    
def get_published_time(s):    
    unit_list = ["weeks", "days", "hours", "minutes", 
                 "seconds", "milliseconds", "microseconds"]
    now = datetime.now()
    parsed_s = [s.split()[:2]]
    unit = parsed_s[0][1]
    if unit in unit_list:
        time_dict = dict((fmt,float(amount)) for amount,fmt in parsed_s)
    elif unit+"s" in unit_list:
        time_dict = dict((fmt+"s",float(amount)) for amount,fmt in parsed_s)
    elif unit in ["month", "months"]:
        time_dict = dict(("days",float(amount)*30) for amount,fmt in parsed_s)
    elif unit in ["year", "years"]:
        time_dict = dict(("days",float(amount)*365) for amount,fmt in parsed_s)  
    else:
        return now   
    dt = datetime.now() - timedelta(**time_dict)
    return dt
        
def get_views_count(s):
    count = ''.join(re.findall(r'(\d+)', s))
    # get count in millions
    count_m = round(float(count)/1E6, 2)
    return count_m

def transform_data(data):
    # remove duplicates
    data.dropna(inplace=True)
    # remove rows where at least one element is missing 
    data.drop_duplicates(inplace=True)
    # add column of current ranking
    data.index += 1
    data = data.rename_axis('rank').reset_index()
    # add column of extracted at
    data["extracted_at"] = datetime.now()
    # convert text to other formats
    data["length_minutes"] = data["lengthText"].apply(get_length_minutes)
    data["published_time"] = data["publishedTimeText"].apply(get_published_time)
    data["views_millions"] = data["viewCountText"].apply(get_views_count)
    # remove unneeded columns
    data.drop(columns=["lengthText", "publishedTimeText", "viewCountText"], inplace=True)
    return data
    
def connect():
    # get private data such as api keys, mysql username/password from Environment variables
    user = os.environ["mysql_user"]
    pwd = os.environ["mysql_pwd"]
    host= os.environ["aws_endpoint"]
    port = os.environ["mysql_port"]
    database= os.environ["mysql_db"]
    table_name = os.environ["mysql_table"]
    engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{database}")
    connection = engine.connect()
    # create the destination table if not exist
    if not engine.dialect.has_table(connection, table_name):
        create_video_table(engine, table_name)
    return connection
    
def create_video_table(engine, table_name):
    metadata = MetaData(engine)
    # create a table with the appropriate Columns
    Table(table_name, metadata,
          Column('Id', Integer, primary_key=True, autoincrement=True, nullable=False), 
          Column('rank', Integer), Column('channelId', String(255)),
          Column('channelName', String(255)), Column('title', String(255)),
          Column('videoId', String(255)), Column('length_minutes', Float),
          Column('views_millions', Float), Column('published_time', DateTime),
          Column('extracted_at', DateTime))
    # implement the creation
    metadata.create_all()   
   
def load_data(data):
    connection = connect()
    try:   
        data.to_sql(os.environ["mysql_table"], if_exists='append', con=connection, index=False)
        print("Success")
    except:
        print("Error occurred")
    connection.close()
        
def lambda_handler(event, context):
    df_videos = extract_data()
    df_videos_clean = transform_data(df_videos)
    load_data(df_videos_clean)

