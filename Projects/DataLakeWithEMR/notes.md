# Project: Data Lake with EMR

## Task
- Import raw data from S3, process into dimensional tables and export back to S3

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
    S3://udacity-dend/song_data/A/B/C/TRABCEI128F424C983.json
    S3://udacity-dend/song_data/A/A/B/TRAABJL12903CDCF1A.json
    ```

**Log data set**
- JSON files
- each file contains logs for a particular **day**
  - **Note: is there any way to import data more quickly given that data is already partitioned in a convenient way?**
- Stored in S3 like:
    ```
   S3://udacity-dend/log_data/2018/11/2018-11-12-events.json
   S3://udacity-dend/log_data/2018/11/2018-11-13-events.json
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

### EMR required configuration

- Release: emr-5.20.0 or later
- Applications: Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0
- Instance type: m3.xlarge
- Number of instance: 3
- EC2 key pair: Proceed without an EC2 key pair or feel free to use one if you'd like


---

## Preliminary Notes
- The only reasons we're using EMR are because it's accessible anywhere by anyone and because it can handle a lot of data, but the pipeline can *just as easily be run locally* in Spark local mode. **Therefore, I should simply work with S3 to begin with and get my code running, then try it on EMR and debug from there.**

- Note: there is some conflicting information about whether I should create an EMR **notebook** or just configure the given etl.py file. But it does look like that whatever I do, I can still upload the code to S3 and run on EMR it via AWS commands (not SSH): https://knowledge.udacity.com/questions/84180
  - **Let's develop in a notebook for ease of modular testing, then put it all in a python script**

**Formatting of Output**
Note these guidlines on writing to S3:
- Each of the five tables are written to parquet files in a separate analytics directory on S3.
- Each table has its own folder within the directory.
- Songs table files are partitioned by year and then artist.
- Time table files are partitioned by year and month.
- Songplays table files are partitioned by year and month.
---

## To-dos:

**ENVIRONMENT SETUP**
- [ ] Decide how I want to build tables (and in what format the results will be stored)
  - Question: Should I partition the resulting S3 tables somehow? By date or by name like the source data is?
- [ ] Set up EMR + Notebook Infrastructre-as-code such that I can run it all via boto3 in a local python notebook
  - I already have code for EMR setup + breakdown.
  - I can upload my Python notebook to S3
  - Can then execute notebook using [this reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.start_notebook_execution)
    - See [this reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_notebook_execution) for how to query status of notebook (e.g., seeing when done)
- [ ] Can I use concurrent.futures code to parallelize my writing to S3 and speed it up?
  - Looks like that is a python native package - so yes!

**CREATE TABLES**
- [ ] Write `CREATE TABLE` statements
  - How best to do this? I don't *HAVE* to use SQL, but it might be easiest.
    - Brainstorming: At the core, I need to combine elements of 2 different JSON files, but I need to combine them based on a common key.
    - **IDEA:** What if I read in all log files, created partial tables, then I read in song data separately and add columns to the tables as needed?
      - Might allow me to drop raw log data earlier - saving memory.

**BUILD ETL**
- [ ] Finish `etl.py` to load data from S3 to EMR
  - Needs explicit S3 access via IAM?
  - Should do this as batch? Or streaming?
  - Use concurrent.futures to parallelize? or Spark (network call, so Spark might not be best choice)?
- [ ] Finish `etl.py` to transform data from JSON to parquet in different files
  - Input will be 2 streams of JSON files, output will be 5 outputs of parquet files
  - Cannot output until all JSON files have been ingested (output requires JOINs)
    - Do some 'tables' only rely on songs or log data? if so, could output those earlier...
- [ ] Finish `etl.py` to load data from EMR to S3
  - - Use concurrent.futures to parallelize? or Spark (network call, so Spark might not be best choice)?
- [ ] Test and **ensure no duplicates!** (see Redshift project for tips)
- [ ] Write a docstring for each function (see Google style from Redshift project)
- [ ] Run analytics queries
- [ ] Delete EMR cluster

**DOCUMENT**
- [ ] Create `README.md` with:
  - [ ] Purpose of database in the context of Sparkify as a startup + their analytical goals
  - [ ] Justify database schema
  - [ ] [Optional] Provide example queries + results for song play analysis

---

## Feedback from Reviewer of Project
