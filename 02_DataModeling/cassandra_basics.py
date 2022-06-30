'''
Example code for working with a Apache Cassandra database in Python
'''
import sys
import cassandra

from cassandra.cluster import Cluster

#Note: I set up my docker-compose file to start cassandra and expose it at the following IP address + port.
#On a production system, this address could simply be changed to the production database instance.
cluster = Cluster(contact_points=['127.0.0.1'],port=9042)
session = cluster.connect()

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

session.execute(
    '''
    CREATE TABLE IF NOT EXISTS music_data
    (year int,
    artist text,
    album text,
    PRIMARY KEY (year,artist,album))
    '''
)

insert_query = '''INSERT INTO music_data (year,artist,album) VALUES (%s, %s, %s)'''

session.execute(insert_query,(1970,"The Beatles","Let it Be"))
session.execute(insert_query,(1965,"The Beatles","Rubber Soul"))

result = session.execute("SELECT * FROM music_data")
print(result.all())

session.execute("DROP TABLE music_data")

session.shutdown()
cluster.shutdown()


'''
sudo docker run -p 127.0.0.1:9042:9042 --rm -d --name cassandra --hostname cassandra_container  cassandra

See this website for transforming docker run commands into docker-compose.yml files: https://www.composerize.com/
'''