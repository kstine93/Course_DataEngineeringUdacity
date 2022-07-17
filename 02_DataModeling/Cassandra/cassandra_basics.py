'''
Example code for working with a Apache Cassandra database in Python
'''
import sys
import pandas as pd
import cassandra
from cassandra.cluster import Cluster

#Note: I set up my docker-compose file to start cassandra and expose it at the following IP address + port.
#On a production system, this address could simply be changed to the production database instance.
cluster = Cluster(contact_points=['127.0.0.1'],port=9042)
session = cluster.connect()

data = pd.read_csv('music_data.csv')
print(data)

#Creating Keyspace
try:
    session.execute('''
    CREATE KEYSPACE IF NOT EXISTS kevin_keyspace
    WITH REPLICATION =
    {'class':'SimpleStrategy','replication_factor':'1'}
    ''')

    session.set_keyspace('kevin_keyspace')
except Exception as e:
    print(e)

#Creating Table:

'''
music_years
- Intended to serve queries about years of music, possibly filtered by artist
- SELECT * FROM music_years WHERE year=1970 AND artist='The Beatles'
- Partition key: year
- Clustering keys:
    1. artist
    2. album
'''
session.execute(
    '''
    CREATE TABLE IF NOT EXISTS music_years
    (year int,
    artist text,
    album text,
    PRIMARY KEY (year, artist, album))
    '''
)

'''
music_albums
- Intended to search for the albums of specific artists, possibly filtering by year
- SELECT * FROM music_years WHERE artist='The Beatles' AND year=1970
- Partition key: artist
- Clustering keys:
    1. year
    2. album
'''
session.execute(
    '''
    CREATE TABLE IF NOT EXISTS music_albums
    (artist text,
    year int,
    album text,
    PRIMARY KEY (artist,year, album))
    '''
)

years_insert = '''INSERT INTO music_years (year,artist,album) VALUES (%s, %s, %s)'''
album_insert = '''INSERT INTO music_albums (artist,year,album) VALUES (%s, %s, %s)'''

#Note: Itertuples is much faster than iterrows
#https://medium.com/swlh/why-pandas-itertuples-is-faster-than-iterrows-and-how-to-make-it-even-faster-bc50c0edd30d
#even more so if we include 'name=None' (but this doesn't allow named indexing)
for row in data.itertuples():
    session.execute(years_insert,(row.year,row.artist_name,row.album_name))
    session.execute(album_insert,(row.artist_name,row.year,row.album_name))

queries = [
    "SELECT * FROM music_albums WHERE artist = 'The Monkees'",
    "SELECT * FROM music_years WHERE year = 1965 AND artist='The Beatles'"
]
for query in queries:
    result = session.execute(query)
    for row in result:
        print(row)

session.execute("DROP TABLE music_years")
session.execute("DROP TABLE music_albums")

session.shutdown()
cluster.shutdown()


'''
sudo docker run -p 127.0.0.1:9042:9042 --rm -d --name cassandra --hostname cassandra_container  cassandra

See this website for transforming docker run commands into docker-compose.yml files: https://www.composerize.com/

For using the docker-compose file, I can run `sudo docker-compose up -d`
'''