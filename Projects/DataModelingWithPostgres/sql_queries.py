# DROP TABLES

generic_table_drop = "DROP TABLE IF EXISTS {table}"
tables = ['songplays','users','songs','artists','time']

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE songplays
    (songplay_id SERIAL PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    location varchar,
    user_agent varchar)
""")

user_table_create = ("""
    CREATE TABLE users
    (user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar)
""")

song_table_create = ("""
    CREATE TABLE songs
    (song_id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar,
    year integer,
    duration real NOT NULL)
""")

artist_table_create = ("""
    CREATE TABLE artists
    (artist_id varchar PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude double precision,
    longitude double precision)
""")

time_table_create = ("""
    CREATE TABLE time
    (start_time timestamp PRIMARY KEY,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday varchar)
""")


#QUERY LIST
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

# INSERT RECORDS
generic_table_insert = """INSERT INTO {table} ({attributes}) VALUES (~values~)"""

def format_insert_with_values(query,num_values):
    """Helper function to dynamically insert string interpolation characters - allows
    the generic_table_insert above to by used dynamically
    """
    #Adds the placeholder '%s' values that can be used to dynamically insert values
    return query.replace("~values~",", ".join(['%s']*num_values))

# FIND SONGS
#This query intended to pair song IDs and artist IDs if the song name matches, the artist name matches,
#and the artist_id given by the song and artists tables are identical
song_select = ('''SELECT s.song_id, a.artist_id 
        FROM songs s, artists a 
        WHERE a.name = %s AND
        s.title=%s
        AND a.artist_id = s.artist_id'''
)