# Sparkify Database Setup - Cassandra

## An explanation of the files in the repository

### Production
- event_data
  - Directory holding user event data (from user sessions) files
- docker-compose.yml
  - File for setting up a Cassandra instance in a Docker container
- Project_1B_ Project_Template.ipynb
  - Notebook containing all code for data manipulation, Cassandra setup, and querying Cassandra
- README.md
  - This document; description of this application
- images
  - Directory holding images used in Markdown of Python notebook

### Development
- notes.md
  - Personal notes taken while completing this project

---

## Problem Overview
This project had the following brief:

```
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.**

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.
```

Specifically, 'Sparkify' wanted to be able to query data in 3 ways:
```
Query 1: Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

```
The remainder of this file shows why solutions for these problems were designed in certain ways.

## Solution Design and Implementation
### Query #1
`Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4`

This query wants to filter on `sessionId` and `itemInSession`, so `sessionId` was chosen as a partitioning key and `itemInSession` as a clustering key - so that each partition represents a unique session. Within those sessions, items are ordered by `itemsInSession` which shows the order in which session events occurred.

Partitioning by session should also naturally prevent partitions growing unboundedly, since sessions are usually time-constrained (i.e., only so many events can happen within a session).


**Table for this query:**
```
CREATE TABLE IF NOT EXISTS songs_by_session
(sessionId int,
itemInSession int,
artist text,
song text,
length float,
PRIMARY KEY (sessionId, itemsInSession))
```

**SELECT statement for this query:**
```
SELECT artist,song,length FROM songs_by_session WHERE sessionId = 338 AND itemInSession = 4
```

### Query #2
`Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182`

#### Optimizing this query
It should be possible to change this query above to use the same table we made for query #1.

By running the following code, we can see that `sessionId` is unique *even across multiple users*. This means that if we were to partition nodes according to only `sessionId` that would already uniquely identify users. It's probably therefore **unnecessary to filter by BOTH sessionId AND userId**
```
#Shows that each sessionId only has 1 unique userId associated with it
df = pd.read_csv('event_datafile_new.csv')
df.groupby('sessionId')['userId'].nunique().describe()
```
It would be great if query #2 could also filter only with `sessionId`, since that would allow the table above to partition only on `sessionId` and satisfy both query #1 and query #2.

This is a question which needs clarification from Sparkify. However, since we can't get that in this project, we need to proceed under the assumption that we **need** to filter by userId in this second query.

>UPDATE POST-PROJECT:
The feedback I got from submitting this project recommended that I use both **userId** and **sessionId** as a composite partitioning key (not just **userId**). This will potentially create a lot of partitions, but the reviewers noted that that is preferable to having (potentially) very large partitions which could result from only partitioning on **userId**. The code below reflects this change.

**Table for this query:**
```
CREATE TABLE IF NOT EXISTS songsUser_by_sessionUser
  (userId int,
  sessionId int,
  itemInSession int,
  artist text,
  song text,
  firstName text,
  lastName text,
  PRIMARY KEY ((userId, sessionId), itemInSession))
```

**SELECT statement for this query:**
```
SELECT artist,song,firstName,lastName FROM songsUser_by_sessionUser WHERE userId = 10 AND sessionId = 182 ORDER BY itemInSession
```
### Query #3
`Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'`

This query wants to filter on `song`, but *only* filtering on song might actually be a bad idea:
- Songs with the same name can belong to different artists
- There are *millions* of songs - so if we only partition only by song, we would have millions of partitions (perhaps too many)

A solution to the above points is to instead partition **based on artist** - and insist that analysts specify for which artist they want to organize song data on. The table and query below reflect this change.

>UPDATE POST-PROJECT:
The feedback I got from reviewers did acknowledge that using **artistId** might be preferable. However, they also noted that since the query doesn't specify artist at all, we need to assume in this instance that the analysts **do not care which artist made the song**. I admit I still don't see the use case for this, and I suspect it's a poorly-created query, but I am reverting the code back so that artist is no longer taken into account. The code below reflects this change.

**Table for this query:**
```
CREATE TABLE IF NOT EXISTS users_by_song
  (song text,
  userId int,
  firstName text,
  lastName text,
  PRIMARY KEY (song, userId))
```

**SELECT statement for this query:**
```
SELECT firstName,lastName FROM users_by_song WHERE artist = 'The Black Keys' AND song = 'All Hands Against His Own'
```

## Other Changes to Code
### Re-creating CSV creation to use less memory
The code provided in the `Project_1B_ Project_Template.ipynb` notebook takes the separate data files in the `event_data` directory and re-combines it to create a single CSV for inserting data into Cassandra.

The original version of this code read in all separate files, stored them in memory as 2-dimensional list, and then re-wrote them to the new file. This approach will probably not scale once we have more data- since we will run out of memory to hold all of the data before we write it.

The new version of the code I wrote writes each line of data from the separate files immediately to the new file, so very little is accumulated in memory (in theory).

### Re-creating data insert functions to be re-usable
The original code provided for reading in CSV data to be inserted into Cassandra was not reusable. I re-wrote the code to be a generic, callable function that I could use for inserting data into any of my tables.

### Fixing corrupt text encoding
Using the `ftfy` package, corrupt strings in the original CSV files (e.g., "La Boulette (GÃÂ©nÃÂ©ration Nan Nan)") were fixed (e.g., "La Boulette (Génération Nan Nan)")
