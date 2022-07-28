# Project: Data Modeling with Cassandra

## Task
- Client wants to query `what songs users are listening to`
  - I need to create a database to serve these queries; **client has provided specific queries they intend to use**
- Flow: CSV files --> ETL.py script --> Cassandra database

## Data Formats
- CSV files
  - Separate files represent separate DAYS
    - *Note: might be a good idea to partition on days anyway - possible to take advantage of the fact that partitions and files are similarly divided (e.g., processing data in batches)?*
    - 
### Desired Schema

## Preliminary Notes

## To-dos:
1. Design tables
2. Create Cassandra keyspace + tables
   1. Use `IF NOT EXISTS` to avoid re-creating tables
      1. Why?
3. Design INSERT statements
   1. Also need ON CONFLICT? If yes, consider what policy would make sense
4. Build ETL to iterate over CSV files and
   1. Clean / transform data (if needed)
   2. load into Cassandra tables
5. Go back to original CSV file transformation and try to optimize (see my markdown notes there)


---

## Feedback from Reviewer of Project

### Sequence of attributes in CREATE and INSERT statements
>*"The sequence of the columns in the CREATE and INSERT statements should follow the order of the COMPOSITE PRIMARY KEY and CLUSTERING columns. The data should be inserted and retrieved in the same order as how the COMPOSITE PRIMARY KEY is set up. This is important because Apache Cassandra is a partition row store, which means the partition key determines which any particular row is stored on which nodeThe sequence of the columns in the CREATE and INSERT statements should follow the order of the COMPOSITE PRIMARY KEY and CLUSTERING columns. The data should be inserted and retrieved in the same order as how the COMPOSITE PRIMARY KEY is set up. This is important because Apache Cassandra is a partition row store, which means the partition key determines which any particular row is stored on which node. In case of composite partition key, partitions are distributed across the nodes of the cluster and how they are chunked for write purposes. Any clustering column(s) would determine the order in which the data is sorted within the partition.
What this mean exactly is for instance if you have table with columns artist_name, song_title, song_length, sessionId, itemInSession & the PRIMARY KEY is COMPOSITE of (sessionId, itemInSession) The creation of TABLE should be such that these columns from PRIMARY KEY are first in the order as:. In case of composite partition key, partitions are distributed across the nodes of the cluster and how they are chunked for write purposes. Any clustering column(s) would determine the order in which the data is sorted within the partition.
What this mean exactly is for instance if you have table with columns artist_name, song_title, song_length, sessionId, itemInSession & the PRIMARY KEY is COMPOSITE of (sessionId, itemInSession) The creation of TABLE should be such that these columns from PRIMARY KEY are first in the order as:"*
```
CREATE TABLE IF NOT EXISTS sessions sessionId int, itemInSession int, artist_name text, song_title text, song_length float, PRIMARY KEY(sessionId, itemInSession))
```
>*Similar order should be ensured for inserting as well.
Here are some DataStax documentation you can refer to for the same:
https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/whereClustering.html*

**Notes:**
- I don't fully understand this yet. My original queries did indeed not always order data the same in 'INSERT' statements or 'CREATE TABLE' statements, but my queries did work, which suggests that Cassandra was re-arranging the columns as needed (like SQL does). Maybe this is instead a comment on readability (i.e., for other users, it's important that data is ordered the same way). I say this also because the [documentation that the reviewer provided](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/whereClustering.html) also provides an example of a CREATE TABLE statement where the order of columns in the PRIMARY KEY doesn't match the order of columns in the table itself. Additionally, none of my original queries produced error statements (not a guarantee though)
- For now, I am re-ordering the columns so that they are always in the same order. However, it would be nice to learn more about this (whether it's for readability or not) so that I can learn when I can break this rule if needed.

### Query #2 PRIMARY KEY
>*You have used PRIMARY KEY(userId, sessionId, itemInSession). However, note that this is not an optimal choice of partition key (currently only userId) because sessions belonging to the same user might be in different nodes. This will cause a performance issue if the database is very large. Therefore we should use both userId and sessionId as partition keys so sessions from the same user are stored together. You can do this by PRIMARY KEY((userId, sessionId), itemInSession).*

**Notes:**
- I don't understand this comment yet. I thought that by partitioning only on userId, I could store all sesssions from a single user on one partition (on one node). I thought that was how Cassandra worked- so that only accessing 1 partition (in this case a single user) would be maximally efficient.
- The reviewer says `we should use both userId and sessionId as partition keys so sessions from the same user are stored together`. I don't quite understand this. From what I saw of the data, 'sessionId' is already unique to a specific 'userId', so if I partition by 'userId', it should already be the case that all sessions for that user are stored on the same partition. What am I missing here?

### Query #3 PRIMARY KEY
>*"The requirement is to get every user name who listened to a particular song. Here the user and the song played would uniquely identify each record.
The column, user id could uniquely identify each user. So the columns user_id and song_title are sufficient to identify each record uniquely, leading us to PRIMARY KEY (song_title, user_id).
What you did will give the expected results, but considering the millions of records and the storage requirements using just song and userid is a more efficient solution"*

**Notes:**
- I don't agree with this comment, namely `Here the user and the song played would uniquely identify each record`. I don't think this will be true in a real-world scenario. As I mentioned in my README.md file:
>*"This query wants to filter on `song`, but *only* filtering on song might actually be a bad idea:
>- Songs with the same name can belong to different artists
>- There are *millions* of songs - so if we only partition only by song, we would have millions of partitions (perhaps too many)"*
