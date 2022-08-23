
import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#Note: purposefully not including distribution or sorting strategy for staging tables
#Since the data will only be queries once (to populate analytics tables), optimizing the table structure is probably
#not worth the time.

#NOTE: For Redshift, "VARCHAR(50)" creates a column that can hold 50 BYTES - NOT 50 CHARACTERS.
#I am doubling all VARCHAR definitions, since English characters are 1-2 bytes (e.g., VARCHAR(50) -> VARCHAR(100))

staging_events_table_create = ("""
    CREATE TABLE events_staging
    (
        artist              VARCHAR(2000),
        auth                VARCHAR(30) NOT NULL,
        firstName           VARCHAR(200),
        gender              VARCHAR(2),
        itemInSession       INTEGER,
        lastName            VARCHAR(200),
        length              REAL,
        level               VARCHAR(20) NOT NULL,
        location            VARCHAR(100),
        method              VARCHAR(14) NOT NULL,
        page                VARCHAR(60) NOT NULL,
        registration        BIGINT,
        sessionId           INTEGER,
        song                VARCHAR(2000),
        status              INTEGER NOT NULL,
        ts                  BIGINT NOT NULL,
        userAgent           VARCHAR(400),
        userId              INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE songs_staging
    (
        num_songs           INTEGER NOT NULL,
        artist_id           VARCHAR(50) NOT NULL,
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     VARCHAR(200),
        artist_name         VARCHAR(2000),
        song_id             VARCHAR(50) NOT NULL,
        title               VARCHAR(2000) NOT NULL,
        duration            REAL NOT NULL,
        year                INTEGER NOT NULL
    )
""")

#Note: Sorting and distributing on songplay_id for fact table.
songplay_table_create = ("""
    CREATE TABLE songplays
    (
        songplay_id         INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time          TIMESTAMP NOT NULL distkey sortkey,
        user_id             INTEGER,
        level               VARCHAR(20) NOT NULL,
        song_id             VARCHAR(50),
        artist_id           VARCHAR(50),
        session_id          INTEGER,
        location            VARCHAR(200),
        user_agent          VARCHAR(400)
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE users
    (
        user_id             INTEGER PRIMARY KEY sortkey,
        firstName           VARCHAR(300),
        lastName            VARCHAR(300),
        gender              VARCHAR(2),
        level               VARCHAR(20) NOT NULL
    ) diststyle auto;
""")

song_table_create = ("""
    CREATE TABLE songs
    (
        song_id             VARCHAR(50) PRIMARY KEY sortkey,
        title               VARCHAR(2000) NOT NULL, --Some VERY long song names
        artist_id           VARCHAR(50) NOT NULL,
        duration            REAL NOT NULL,
        year                INTEGER NOT NULL
    ) diststyle auto;
""")

artist_table_create = ("""
    CREATE TABLE artists
    (
        artist_id           VARCHAR(50) PRIMARY KEY sortkey,
        name                VARCHAR(2000), --Some VERY long artist names
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     VARCHAR(200)
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE time
    (
        start_time          TIMESTAMP PRIMARY KEY distkey sortkey,
        hour                SMALLINT NOT NULL,
        day                 SMALLINT NOT NULL,
        week                SMALLINT NOT NULL,
        month               SMALLINT NOT NULL,
        year                SMALLINT NOT NULL,
        weekday             SMALLINT NOT NULL
    ) diststyle key;
""")

# STAGING TABLES COPY FROM S3

staging_events_copy = (f"""\
    COPY events_staging FROM '{config['S3']['LOG_DATA']}'\
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'\
    json 'auto';\
""")

staging_songs_copy = (f"""\
    COPY songs_staging FROM '{config['S3']['SONG_DATA']}'\
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'\
    json 'auto';\
""")

# FINAL TABLES INSERT FROM STAGING

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT
        (SELECT TIMESTAMP 'epoch' + l.ts * INTERVAL '0.001 seconds' AS ts),
        l.userId,
        l.level,
        s.song_id,
        s.artist_id,
        l.sessionId,
        l.location,
        l.userAgent
    FROM events_staging l
    JOIN songs_staging s ON (l.artist = s.artist_name AND l.song = s.title)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, firstName, lastName, gender, level)
    SELECT
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM events_staging
    WHERE userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,title,artist_id,duration,year)
    SELECT
        song_id,
        title,
        artist_id,
        duration,
        year
    FROM songs_staging
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,name,artist_latitude,artist_longitude,artist_location)
    SELECT
        artist_id,
        artist_name,
        artist_latitude,
        artist_longitude,
        artist_location
    FROM songs_staging
""")

time_table_insert = ("""
    INSERT INTO time (start_time,hour,day,week,month,year,weekday)
    WITH converted_ts AS (
        SELECT TIMESTAMP 'epoch' + ts * INTERVAL '0.001 seconds' AS ts
        FROM events_staging
    )
    SELECT
        ts,
        EXTRACT(hour FROM ts) AS hour,
        EXTRACT(day FROM ts) AS day,
        EXTRACT(week FROM ts) AS week,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts) AS year,
        EXTRACT(weekday FROM ts) as weekday
    FROM converted_ts
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
