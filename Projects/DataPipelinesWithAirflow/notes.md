# Project: Data Pipelines with Airflow

## Task
The music company "Sparkify" wants a data pipeline for its S3-to-Redshift data flow(s) that has:
- Reusable tasks
- Can be monitored
- Allow easy backfills
- Has data quality checks


1. ~~Configure the DAG with:~~
   1. DAG does not have dependencies on past runs
   2. On failure, tasks are retried 3 times
   3. Catchup is turned off
   4. Do not email on retrys
2. DAG task dependencies must reflect this structure:
    <img src="./media/target_dag.png">
3. Implement the placeholder operators provided by Udacity:
   1. **Stage Operator**
      1. Able to load any .json files from S3 to Redshift. Creates (formats?) and runs a SQL COPY statement based on given parameters (including S3 location and target table name + location). Contains templated field to allow it to load S3 files that are named according to timestamp.
   2. **Fact & Dimension Operators**
      1. Utilize SQL helper class to run data transformations. Takes SQL statement as input and a target database to run the query against. Also has target table to receive results (e.g., of a "CREATE TABLE" query).
      2. Dimension table loading are often done with a TRUNCATE + INSERT command (i.e., empty table, then fill back in), but not always. Consider adding a parameter "empty_first:bool=False" that allows users to specify whether to do this.
      3. Fact tables should only allow appends, ideally.
   3. **Data Quality Operator**
      1. Receives SQL-based test cases along with expected results, then executes the tests
      2. If there is no match between expected result(s) and query results, raise exception and retry a finite number of times.

### Data sources:
**(Copy these to your own bucket first):**
- `aws s3 sync s3://udacity-dend/song-data/ s3://kstine-airflow-pipeline/project/song-data/`
- Log data: `s3://udacity-dend/log_data`
- Song data: `s3://udacity-dend/song_data`

### Deliverables:
- [ ]

---

## Preliminary Notes

---

## To-dos:


## Feedback from Reviewer of Project
(placeholder)