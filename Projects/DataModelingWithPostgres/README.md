# Sparkify Database Setup

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
Do some explanation as to why it's nice to abstract things like table names + attributes away from creation script (makes script reusable to an extent)
Maybe also note that for next steps, also abstract away database creation details.
### Production
- data/song_data
  - Nested directory holding song information files
- data/log_data
  - Nested directory holding user log information files
- create_tables.py
  - Script to (re-)create Postgres datbase & tables
- etl.py
  - Script to populate Postgres tables with log and song data
- README.md
  - This document; description of this application

### Development
- etl.ipynb
  - For testing aspects of etl.py. Only used for development purposes.
- test.ipynb
  - Provides testing of database and data insertion
    - Should ideally be adapted to be part of QA in future iteration of this code

---

## State and justify your database schema design and ETL pipeline

### Schema Design
This database is set up to optimize the querying of the fact table 'songplay' (i.e., we have de-normalized the schema to allow these data to be queried with fewer JOINs).

This was most easily achievable through a 'star schema' design where the innermost fact table ('songplays') held all the foreign keys and the most critical information to be queried (in many use cases, only the songplay table would be needed; dimension tables would be brought in only to supply details (e.g., the actual name of the artist)).

### Extract-Transform-Load (ETL) Pipeline Design
I made several changes to the structure of the code in an attempt to make the code less vulnerable to breaking and easier to re-use in other projects.

---

#### 1. Specifying attributes and values in the same location for 'INSERT' statements
For SQL insert queries, it's very important that the order of the attributes is the SAME as the order of the values - even more so when attributes are almost all the same type (varchar), since mis-ordering the values could mean that you're inserting data to the wrong column, but you would receive no error message.

To make the insert operations safer, I changed the query strings so that users would be forced to provide the list of attributes  AND the list of values in the same operation (in etl.py) - to limit the risk that they become out of order.

Next Steps: we could extend this further in the future to have a dict which shows the exact mappings of the attributes (e.g., "user_agent") onto what these values are called in the data files (e.g., "UserAgent") to further prevent accidental mis-orderings.

---

#### 2. Making SQL queries generic

To make the code (a little) more reusable, I also made the 'TABLE DROP' query strings generic.

In general, it would be cool to build a Python class or wrapper library which would allow dynamic building of Postgres query strings - but possibly not worth the effort.

---

#### 3. Mixing string interpolation (%s) with psycopg2.sql identifiers
Python string interpolation relies on the order of values to insert data into the correct location. I don't like the idea of data needing to be exactly ordered to work correctly - particularly if mis-ordered data might not produce an error (e.g., if we mis-order attributes in an INSERT statement for a table with all VARCHAR atttributes).

psycopg2 also offers syntax like `sql.Identifier(var)` to dynamically insert **some** data like table names and attribute names. I use this whenever possible to avoid relying on string interpolation.

---

#### 4. Not using Pandas data frames
I opted to not use Pandas data frames in the ETL pipeline. Since the data is already well-structured in JSON form, I didn't see the need to re-structure it from a dictionary into a data frame (extra operation for not much benefit).

However, I have not tested the performance of using a Dataframe vs. dictionary - it could be that Dataframe indexing is faster and therefore worth the cost of transformation.

---

#### 5. Breaking code into smaller functions with a single purpose
Especially in etl.py, I divided the functions to extract data and insert into separate tables into their own functions (e.g., `insert_into_artists()`). This allows benefits like:
- Placing these functions in different files to make etl.py shorter.
- More easily abstracting these methods into a class or library in the future.
- Running insert statements independently of each other for different use cases.

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
