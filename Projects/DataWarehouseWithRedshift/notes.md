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
      - Reason: the JSON files are not structured as our end-state tables need to be. The first step loads the JSON files into tables created to match the JSON structure. The second step uses INSERT SELECT statements to re-arrange the data and put it into end-state tables.
  - `sql_queries.py` for defining SQL statements
  - `README.md` for providing overview of my decisions

**On Staging Tables:**
The data being used here is the same as used for the Cassandra project - I am loading in log / song files and making a star schema from the data.
However, in the Cassandra project, I was loading in local files and pushing them to Cassandra - my Python script was the ETL. Here, by contrast, I am loading *remote* files onto a *remote* database - and I have no default ETL platform.

I think the staging tables can be my ETL platform. First, I can load all data into 'Log' and 'Song' tables, since that is how the files are already organized.
Then, I can INSERT INTO a new Schema that is star-shaped - all using SQL or Redshift commands (no Python)

**Note: How could this setup be tweaked for data streaming? Append to staging tables and then draw from staging to analytics until staging is empty?**

---

## To-dos:

**CREATE TABLES**
- [x] Design Schemas for fact + dimension tables
   - [x] They've already been designed - I just need to maybe decide on sorting + partitioning keys. Take a look at Cassandra project...
- [x] Write `CREATE TABLE` statements
- [x] Finish `create_tables.py` to connect to database & create tables
- [x] **Required:** `DROP TABLE IF EXISTS` statements for resetting entire database
  - **personal note:** I do not like these statements. I want to know if I'm trying to re-create a database that already exists.
- [x] Create IAM role which has S3 read access
- [x] Launch Redshift cluster
- [x] Add Redshift database + IAM role details to `dwh.cfg` file
- [x] Create tables in redshift.
- [x] Query to make sure tables were created correctly.

**BUILD ETL**
- [x] Finish `etl.py` to load data from S3 to Redshift staging tables
- [x] Finish `etl.py` to load data from staging tables to analytics tables
- [x] Run analytics queries
- [x] Delete Redshift cluster

**DOCUMENT**
- [x] Create `README.md` with:
  - [x] Purpose of database in the context of Sparkify as a startup + their analytical goals
  - [x] Justify database schema
  - [x] [Optional] Provide example queries + results for song play analysis

---

## Feedback from Reviewer of Project


- Please remove NOT NULLs from the staging tables.
  - "The staging tables should be an exact copy of the data from JSON or CSV files. It is better not to apply any type of constraint or filtering at this stage
    - **NOTE: But why? I wonder if this is just a convention. Right now I don't see any reason why not filtering at this stage would be any worse than filtering at another stage.**
- Please use 's3://udacity-dend/log_json_path.json' as JSON format while copying events data. For songs data, the format option JSON 'auto' is ok.
  - **Note: Ah- ok, so there is another file in the S3 that is used as a template for communicating what the format of the file uploads will be. I've downloaded this for future reference here: `./redshift_project_json_format.json`**
- Handle duplicates (Use SELECT DISTINCT to filter duplicate entries.)
  - **NOTE: Good point. Since Redshift does not enforce uniqueness for tables, if I do not enforce it I will get duplicates which would be bad.**
- Rewrite the songplays_insert query so that the records are filtered based song title, artist name and song duration.
- Add docstrings to each function in etl.py and create_tables.py
- Please note that Redshift does not enforce unique, primary-key, and foreign-key constraints. Even though they are informational only, the query optimizer uses those constraints to generate more efficient query plans.
  - https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-defining-constraints.html
  - http://www.sqlhaven.com/amazon-redshift-what-you-need-to-think-before-defining-primary-key/