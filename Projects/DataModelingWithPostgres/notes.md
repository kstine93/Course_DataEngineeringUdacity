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

### Preliminary Notes
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

## To-dos:
1. ~~Decide whether I really want to develop locally~~ -> **No, I will develop in given environment, downloading files to preserve progress (I don't trust environment not to reset)**
   1. There could be automated testing based on some aspects of Udacity environment (e.g., user, password for Postgres)
   2. "Project Checklist" does specify that I should use the built-in workspace
   3. I don't really want to gum up my GitHub with code I only half-wrote and also with data.
2. Table Setup
   1. Write create table + drop table statements in sql_queries.py
      1. Q: I want to re-arrange these into JSON format. Can I? Or could it break testing?
   2. Create tables using given script + test tables with given script
      1. **NOTE: Read through these scripts to understand how they work. Consider downloading local copies for my reference**
3. Develop ETL
   1. Study etl.py to see how the original code is set up.
   2. Finish etl.py (using etl.ipynb for development, if you want) so that it correctly reads in and populates tables
      1. Note: "Process_data" udf is what uses os to find and pull files, sending them to be processed.