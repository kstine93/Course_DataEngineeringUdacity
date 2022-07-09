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
- Dimension Tables
  - songs
    - song_id
    - title
    - artist_id
    - year
    - duration
  - users
    - user_id
    - first_name
    - last_name
    - gender
    - level
  - artists
    - artist_id
    - name
    - location
    - latitude
    - longitude
  - time (timestamps of records in songplays fact table, broken down into specific units)
    - start_time
    - hour
    - day
    - week
    - month
    - year
    - weekday
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
   1. ~~Write create table + drop table statements in sql_queries.py~~
      1. Q: I want to re-arrange these into JSON format. Can I? Or could it break testing?
         1. psycopg2 has some odd requirements around how it formats query strings - which in some cases prevent having generic strings that I can insert whatever values I want into. For this reason, making these into JSONs was more effort than payoff.
   2. ~~Create tables using given script + test tables with given script~~
      1. **NOTE: Read through these scripts to understand how they work. Consider downloading local copies for my reference**
         1. Nothing revolutionary here. Although interesting that the creation of a new database required first connecting to an EXISTING database, creating the new database, then closing and re-opening the connection to the new database (weird that you have to connect to a default database first)
3. Develop ETL
   1. Study etl.py to see how the original code is set up.
   2. Finish etl.py (using etl.ipynb for development, if you want) so that it correctly reads in and populates tables
      1. Note: "Process_data" udf is what uses os to find and pull files, sending them to be processed.
   3. Run etl.py to populate tables. Test with test.ipynb (including 'sanity checks' section)
      1. **NOTE: the test.ipynb file has some stuff that I don't understand yet (% operators at beginning of lines - accesses terminal?) Download and explore a bit**
   4. Document process in Readme, including:
       - Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
       - How to run the Python scripts
         - Sequentially, except that creating tables cannot be run again once data is added (will drop and recreate database)
       - An explanation of the files in the repository
         - Do some explanation as to why it's nice to abstract things like table names + attributes away from creation script (makes script reusable to an extent)
           - Maybe also note that for next steps, also abstract away database creation details.
       - State and justify your database schema design and ETL pipeline.
         - NOTE: How do I justify when I didn't build it? Consider re-designing ETL so I can defend it (can't redesign schema)
         - For both ETL + schema, perhaps just point out the reasoning behind the structure (i.e., reference why fact + dimension tables are useful to begin with, normality (or lack thereof); for ETL, study how it works, describe iteration technique + ways in which error prevention is done (i.e., specifying attributes as well as values))
       - [Optional] Provide example queries and results for song play analysis.
         - I should definitely do this, for practice + cementing my understanding
