import os
import glob
import psycopg2
from psycopg2 import sql
import json
import datetime
from sql_queries import *


#Helper Functions
def insert_data_into_table(cur,table,attributes_arr,values_arr,query=generic_table_insert):
    """Inserts given values + given attributes into given Postgres table"""
    query = format_insert_with_values(query,len(values_arr))
    
    formatted_query = sql.SQL(query).format(
        table=sql.Identifier(table),
        attributes=sql.SQL(',').join(create_composable_arr(attributes_arr))
    )
    cur.execute(formatted_query,values_arr)


def create_composable_arr(array):
    """Dynamically creates array of composable elements for psycopg2 SQL commands"""
    return [sql.Identifier(elem) for elem in array]


#Data Insertion functions
def insert_into_artists(cur,data):
    """Inserts data from song data file into the 'artists' table of the 'sparkifydb' Postgres database"""
    #Note: Order of arrays must be the same
    artist_attr = [
        'artist_id',
        'latitude',
        'longitude',
        'location',
        'name'
    ]
    artist_vals = [
        data['artist_id'],
        data['artist_latitude'],
        data['artist_longitude'],
        data['artist_location'],
        data['artist_name']
    ]
    
    #For conflicting primary key, choosing to not overwrite original data:
    query = generic_table_insert + ''' ON CONFLICT (artist_id) DO NOTHING'''
    insert_data_into_table(cur,"artists",artist_attr,artist_vals,query)
    

def insert_into_songs(cur,data):
    """Inserts data from song data file into the 'songs' table of the 'sparkifydb' Postgres database"""
    #Note: Order of arrays must be the same
    song_attr = [
        'artist_id',
        'song_id',
        'title',
        'duration',
        'year'
    ]
    song_vals = [
        data['artist_id'],
        data['song_id'],
        data['title'],
        data['duration'],
        data['year']
    ]
    
    #For conflicting primary key, choosing to not overwrite original data:
    query = generic_table_insert + ''' ON CONFLICT (song_id) DO NOTHING'''
    insert_data_into_table(cur,"songs",song_attr,song_vals,query)


def insert_into_time(cur,next_song_logs):
    """Inserts data from log data file into the 'time' table of the 'sparkifydb' Postgres database"""
    #Note: this array of attributes must be ordered the same as the values in following iteration
    time_attr = [
        'start_time',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday'
    ]
    
    for log in next_song_logs:        
        time_vals = [
            log['ts'], #Question: Will Postgres accept python's 'datetime' format?
            int(log['ts'].strftime("%H")),
            int(log['ts'].strftime("%d")),
            int(log['ts'].strftime("%V")), #week number
            int(log['ts'].strftime("%m")), #"M" is minute, "m" is month
            int(log['ts'].strftime("%Y")),
            log['ts'].weekday()
        ]
        #For conflicting primary key, choosing to not overwrite original data:
        query = generic_table_insert + ''' ON CONFLICT (start_time) DO NOTHING'''
        insert_data_into_table(cur,"time",time_attr,time_vals,query)
    
    
def insert_into_users(cur,next_song_logs):
    """Inserts data from log data file into the 'users' table of the 'sparkifydb' Postgres database"""
    #Note: this array of attributes must be ordered the same as the values in following iteration
    user_attr = [
        "user_id",
        "first_name",
        "last_name",
        "gender",
        "level"
    ]

    for log in next_song_logs:        
        user_vals = [
            log['userId'],
            log['firstName'],
            log['lastName'],
            log['gender'],
            log['level']
        ]
        #For conflicting primary key, choosing to not overwrite original data:
        query = generic_table_insert + ''' ON CONFLICT (user_id) DO NOTHING'''
        insert_data_into_table(cur,"users",user_attr,user_vals,query)


def insert_into_songplays(cur,next_song_logs):
    """Inserts data from log data file into the 'songplays' table of the 'sparkifydb' Postgres database"""
    #Note: this array of attributes must be ordered the same as the values in following iteration
    songplay_attr = [
        "start_time",
        "user_id",
        "level",
        "song_id",
        "artist_id",
        "location",
        "user_agent"
    ]
    
    for log in next_song_logs:
        cur.execute(song_select,(log['artist'],log['song']))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_vals = [
            log['ts'],
            log['userId'],
            log['level'],
            songid,
            artistid,
            log['location'],
            log['userAgent']
        ]
        
        query = generic_table_insert
        insert_data_into_table(cur,"songplays",songplay_attr,songplay_vals,query)

        
#Processing wrapper functions
def process_song_file(cur, filepath):
    """extracting song data from filepath and passing this data to be inserted into 'artists' and 'songs' tables"""
    with open(filepath, 'r') as f:
      data = json.load(f)
    
    insert_into_artists(cur,data)
    insert_into_songs(cur,data)
    

def process_log_file(cur, filepath):
    """extracting log data from filepath, conducting initial cleaning of the file
    and passing this data to be inserted into 'time', 'users' and 'songplays' tables"""
    # open log file
    file = open(filepath)
    contents = file.read()
    contents_arr = contents.splitlines()
    
    # Converting string to dict:
    dict_arr = [json.loads(val) for val in contents_arr]

    # filter by NextSong action
    next_song_logs = [log for log in dict_arr if log['page'] == "NextSong"]

    # convert timestamp column to datetime
    for log in next_song_logs:
        log['ts'] = datetime.datetime.fromtimestamp(log['ts']/1000) #Converting from ms
    
    insert_into_time(cur,next_song_logs)
    insert_into_users(cur,next_song_logs)
    insert_into_songplays(cur,next_song_logs)
    
        
def process_data(cur, conn, filepath, func):
    """recursively finds all JSON files starting from a given directory. Passes these filepaths to a given function
    for additional processing"""
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    
    
def main():
    """Connects to database and passes base directory path strings for finding data files
    to insert into database tables"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

#Code Execution
if __name__ == "__main__":
    main()