# Project: Data Warehouse with Redshift

## Task
- Client wants to query `what songs users are listening to`
  - I need to create a database to serve these queries; **client has provided specific queries they intend to use**
- Flow: JSON files in S3 bucket -> Redshift cluster

*Note: This is conceptually very similar to the previous 2 projects with Postgres and Cassandra, differing mostly only in tooling*

## Data Formats

**Song data set**
- JSON files
- each file is 1 song; looks like:
    ```
    {"num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0}
    ```
- Stored in S3 like:
    ```
    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json
    ```

**Log data set**
- JSON files
- each file contains logs for a particular **day**
  - **Note: is there any way to import data more quickly given that data is already partitioned in a convenient way?**
- Stored in S3 like:
    ```
   log_data/2018/11/2018-11-12-events.json
   log_data/2018/11/2018-11-13-events.json
    ```

### Desired Schema

- Fact table = "Songplays"
  - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*
- Dimension tables
  - users
    - *user_id, first_name, last_name, gender, level*
  - songs
    - *song_id, title, artist_id, year, duration*
  - artists
    - *artist_id, name, location, latitude, longitude*
  - time
    - *start_time, hour, day, week, month, year, weekday*

## Preliminary Notes
- They want a few different files to break up ETL:
  - `create_tables.py` to create tables in Redshift
  - `etl.py` for loading data from S3 into *STAGING* tables on Redshift, and then process those *STAGING* tables into Redshift *ANALYTICS* tables
    - **Note: Why the 2-step process? Maybe that will become clear based on data (does it make more sense to port through EC2 if we need data manipulation?**
  - `sql_queries.py` for defining SQL statements
  - `README.md` for providing overview of my decisions

## To-dos:

**CREATE TABLES**
1. Design Schemas for fact + dimension tables
   1. They've already been designed - I just need to maybe decide on sorting + partitioning keys. Take a look at Cassandra project...
2. Write `CREATE TABLE` statements
3. Finish `create_tables.py` to connect to database & create tables
   1. **Required:** `DROP TABLE IF EXISTS` statements for resetting entire database
      1. **personal note:** I do not like these statements. I want to know if I'm trying to re-create a database that already exists.
4. Create IAM role which has S3 read access
5. Launch Redshift cluster
6. Add Redshift database + IAM role details to `dwh.cfg` file
   1. **Note:** Can I use `configparser` package to also create configs? Would be quite convenient.
7. Create tables in redshift. Query to make sure they were created correctly.

**BUILD ETL**
1. Finish `etl.py` to load data from S3 to Redshift staging tables
2. Finish `etl.py` to load data from staging tables to analytics tables
3. Run analytics queries
4. Delete Redshift cluster

**DOCUMENT**
1. Create `README.md` with:
   1. Purpose of database in the context of Sparkify as a startup + their analytical goals
   2. Justify database schema
   3. [Optional] Provide example queries + results for song play analysis

---

## Feedback from Reviewer of Project