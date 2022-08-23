# Sparkify Database Setup with Redshift

## Purpose of this database
This database allows users to query Sparkify song information and user log data, and combinations of both data sets.

The combination of these data sets into a single database enables us to display more meta-information about songs and artists (e.g., year the song was made, location of artist) while querying log data.

---

## How to Run Python Scripts
1. Run *create_tables.py* **ONCE AND ONLY ONCE**
   1. This script drops and re-creates all tables. It should only be re-run if that data loss is acceptable.
2. Run *etl.py* to fill in the tables with song and log data

---

## An explanation of the files in the repository
- dwh.cfg
  - Configuration file to store data about how to access redshift cluster
- redshiftSetup.ipynb
  - Notebook for setting up Redshift cluster and permissions. **Also for populating dwh.cfg file** and querying tables.
- create_tables.py
  - Script to (re-)create Redshift tables
- etl.py
  - Script to populate Redshift tables with log and song data
- README.md
  - This document; description of this application

---

## Justification of Database Schema & ETL pipeline
The star schema used in this database allows for faster analytical querying at the cost of consistency and storage costs (i.e., data is repeated in multiple places). By contrast, using a 3NF database here would require analytical queries to use many more joins and slow down the process of analysis.
With 'songplays' as the fact table, we are allowing greater querying power for analyzing session data specifically - which is what Sparkify needs in its analytical goals.

The ETL pipeline used here exists mainly within Redshift. The 'staging tables' set up and populated from S3 allowed us to use SQL 'INSERT INTO ... SELECT' statements to do basic transformation and re-organization of data while transferring data from the staging tables to the production tables. If additional transformation were needed, this might go beyond what SQL could accomplish and require a Python script on an EC2 instance to serve as the ETL.

---

## Details on Table Distribution Decisions
Dimension Tables = users, time, artist, song
Fact Table = songplays

Resources:
- [AWS - choosing best distribution](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-best-dist-key.html)
- [AWS - distribution styles](https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html)

### songplays + time
I predict that most of my queries will include the `time` table as a way to filter data. Since most of this filtering will happen on columns *other* than the pure timestamp (e.g., filter according to a certain day, month, year, etc.), a `JOIN` will be required. Since the time table represents the timestamp of *every single user session*, it could grow incredibly large.
>**Decision: distribute and sort `songplays` and `time` tables according to timestamp key**

### artist
Since I am distributing `songplays` alongside `time` already, I won't gain any advantage in query performance from distributing `artists` using a key. The AWS guidelines above state that the ALL distribution is best used for tables which do not change regularly and which are ideally relatively small. Of all the tables I have, `artists` is likely to be the smallest.
>**Decision: use ALL distribution strategy for `artists`, sort on artist_id (key) for faster lookup**

### song + users
Both the `song` and `users` data sets represent information which is likely to be grow frequently over time, and which is quite large- making an ALL distribution strategy unreasonable. However, these tables will be frequently JOINED with the fact table or the `artists` table during queries- making an EVEN distribution strategy also unreasonable.
>**Decision: use AUTO distribution strategy for `song` and `users` tables, so that Redshift can re-distribute them dynamically as it sees fit.**

---

## Songplay analysis querying
### Where were users located during their Sparkify sessions on November 30, 2018?

```
SELECT COUNT(*) freq, location
FROM songplays JOIN time ON songplays.start_time = time.start_time
WHERE time.year = 2018 
AND time.month = 11  
AND time.day = 30 
GROUP BY songplays.location 
ORDER BY freq DESC
```

### What were the most popular songs (i.e., most played) in Q4, 2018?
```
SELECT COUNT(*) freq, songplays.song_id, songs.title 
FROM songplays JOIN time ON songplays.start_time = time.start_time 
LEFT JOIN songs on songplays.song_id = songs.song_id 
WHERE time.year = 2018 
AND time.month BETWEEN 10 AND 12 
GROUP BY songplays.song_id, songs.title 
ORDER BY freq DESC
```
