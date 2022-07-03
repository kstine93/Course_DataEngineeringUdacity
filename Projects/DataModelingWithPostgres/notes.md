# Project: Data Modeling with Postgres

## Task
- Create a Postgres database **designed to optimize queries on song play analysis**
- I will create database schema
  - I will define fact + dimension tables  for a **STAR** schema.
- An ETL pipeline
  - Transfers data from local files to the database

## Data Fomats
### Desired Schema
- Fact Table "songplays"
  - attributes:
    - songplay_id
    - start_time
    - user_id
    - level
    - song_id
    - artist_id
    - location
    - user_agent

### Local Song Data
```
#JSON of song format example:
{
   "num_songs":1,
   "artist_id":"ARJIE2Y1187B994AB7",
   "artist_latitude":null,
   "artist_longitude":null,
   "artist_location":"",
   "artist_name":"Line Renaud",
   "song_id":"SOUPIRU12A6D4FA1E1",
   "title":"Der Kleine Dompfaff",
   "duration":152.92036,
   "year":0
}
```

### Local Activity Data
No example. But shows a dataframe. Maybe an array of objects?

### Notes
- Working with song information to create a database
- **Optimization**
  - How to optimize the database for song play analysis?
    - With Postgres (i.e., not NoSQL) can't take advantage of distributed structure
    - Can de-normalize to an extent to make song play analysis simpler (i.e., use fewer tables)?
  - **ETL Pipeline**
    - Most efficient way to read in and push data?
      - Generator? simple 'read file'?
    - What kinds of transformations necessary?
      - Stored originally in JSON
- Big Questions
  - "What songs are users listening to?"
- Submission
  - I would like to submit via GitHub repo, but I'm currently keeping ALL of my notes in a single REPO. However, I think I can continue developing here and then create a 'dummy' repo just for the purposes of submission in this way: https://stackoverflow.com/questions/29306032/fork-subdirectory-of-repo-as-a-different-repo-in-github


