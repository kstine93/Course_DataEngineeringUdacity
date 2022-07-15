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
NOTE: keep in mind that with NoSQL databases (such as Cassandra) it's not possible to query on a completely ad hoc basis.
Instead of simply creating a table to best represent the data, you have to create the table to best respond to the queries
you intend to use. This is because the table will be distributed across several nodes - so you have to tell Cassandra how
to split the data so it can respond to your queries well.
In Cassandra, it seems that if we want to group data by certain columns, we have to include those columns as part of the key...
So, if for example we want to use a 'WHERE year=1970', then year must be part of my primary (composite) key
'''

#Note: In the situation of a COMPOSITE primary key, the FIRST VALUE is the
#"partition key" and the OTHER VALUES given are the "clustering keys"
#You could also include multiple attributes as partition or clustering key
#by making use of parentheses, e.g:
'''PRIMARY KEY ((year, artist), album)'''
#You need to take care when choosing your keys.
#For PARTITION KEY(S), it's most helpful to choose a column where data is likely to be evenly distributed
#Year could be a good partition key if we assume that whatever we're measuring is regular across years
#Product ID could be a BAD partition key, since a new partition is created for every
#product - so a company with 10,000 products would have 10,000 partitions.

#For CLUSTERING KEY(S), you can only use them to filter data in the SAME ORDER AS THEY'RE USED FOR CLUSTERING
#So, if you specify your clustering keys as "(year,artist)" then the statement
#"...WHERE year = 1980 AND artist = 'Bon Jovi'" is CORRECT and the statement
#"...WHERE artist = 'Bon Jovi'" is WRONG

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
    print(result.all())

session.execute("DROP TABLE music_years")
session.execute("DROP TABLE music_albums")

session.shutdown()
cluster.shutdown()


'''
sudo docker run -p 127.0.0.1:9042:9042 --rm -d --name cassandra --hostname cassandra_container  cassandra

See this website for transforming docker run commands into docker-compose.yml files: https://www.composerize.com/

For using the docker-compose file, I can run `sudo docker-compose up -d`
'''