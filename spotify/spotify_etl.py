import sqlalchemy
import requests
import pandas as pd
import datetime
import sqlite3


#Below is for Validty of the data recieved
def check_if_valid_data(df: pd.DataFrame) -> bool:
    ##Check if the DataFrame is empty
    if df.empty:
        print("No songs Downloaded. Finished Execution")
        return False
    
    ##Performing the Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is Violated")
        
    ## Checking For Nulls
    if df.isnull().values.any():
        raise Exception("Null Values Found")
        
    # ## Check that all the timestamps are of yesterdays date
    # yesterday = datetime.datetime.now() - datetime.timedelta(days = 60)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, "%Y-%m-%d") == yesterday:
    #         raise Exception("Atleast one of the returned song does not come within the last 24hrs")
    # return True


if "__name__" == "__name__":
    

    def run_spotify_etl():
        DATABASE_LOCATION = "sqlite:///home/dmmg/dmmg/Icipe Internship/ETLCropOntologies/DataEngineering/my_played_list.sqlite"
        USER_ID = "Edna"
        TOKEN = "BQCODxrUjgw645YjhiM1GlvZMQTVv9-WNbdbk_Tk1DyGhMiJLseCD9tM7WpK6nHv8vMtriXTHg3OlyYQLn7gxCocIghREvuwVoHppgWxHhnFDBo2EdvrJJSCMGosu9j6n_mA3CONWMYzTH5C00UDYf7ImLwfQhOK2ys2"
        ##Time Today = 1622091307040 ##This is in Unix Milliseconds.
        ## Above needs a spotify account

        headers = {
            "Accept":"application/json",
            "ContentType":"application/json",
            "Authorization":"Bearer {token}".format(token=TOKEN)
            }
    
        today = datetime.datetime.now()
        yesterday = today - datetime.timedelta(days= 1)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
        
        r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after(time)".format(time=yesterday_unix_timestamp), headers=headers)
        
        data = r.json()
        #print(data)
        
        song_names = []
        artist_names = []
        played_at_list = []
        url_songs = []
        timestamps = []

        for song in data["items"]:
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            url_songs.append(song["track"])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])

        song_dict = {
            "song_name" : song_names,
            "artist_name" : artist_names,
            "url_song" :url_songs,
            "played_at" : played_at_list,
            "timestamp" : timestamps

        }

        song_df = pd.DataFrame(song_dict, columns =["song_name", "artist_name", "url_song","played_at", "timestamp"])
        
        ## Checking if the Song is Valid
        if check_if_valid_data(song_df):
            print ("Data Valid Proceed to Loading stage")

        
        ##Loading the data
        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        conn = sqlite3.connect("my_played_list.sqlite")
        cursor =  conn.cursor()
        
        sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            url_songs VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
            )
        """
        
        cursor.execute(sql_query)
        
        print("Opened Database Successfully")
        
        try:
            song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
        except:
            print("Data already exists in the database")
            
        query_test = """
            SELECT * FROM my_played_tracks
        """
        cursor.execute(query_test)
        df = pd.read_sql_query("SELECT * FROM my_played_tracks", conn)
        print(df.head(5))
        conn.close()
        print("Close Database Successfully")
