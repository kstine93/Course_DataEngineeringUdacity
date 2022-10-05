# Sparkify Database Setup with EMR + S3

---

## Purpose of this database
This database allows users to query Sparkify song information and user log data, and combinations of both data sets.

The combination of these data sets into a single database enables us to display more meta-information about songs and artists (e.g., year the song was made, location of artist) while querying log data.

---

## How to Run Python Scripts
1. Running the `EMR_boto3setup.ipynb` notebook (with your own AWS credentials) will set up an EMR cluster in AWS for you
2. The `etl.py` can then either be uploaded to a Jupyter notebook on the EMR cluster (accessible via AWS web UI) or uploaded and submitted to Spark by SSHing into the cluster with an EMR key pair.
3. In either case, the etl.py code should be run just once (and will take 1-2 hours for writing results to S3)

---

## An explanation of the files in the repository
**Production Files**
- EMR_boto3Setup.ipynb
  - Notebook for setting up EMR cluster and permissions.
- etl.py
  - Script to extract, transform and re-load all data from and into S3
- README.md
  - This document; description of this application

**Development Files**
- _testNotebookEMr.ipynb
  - Notebook holding test code for creating etl.py. Has some additional code for testing if needed.
- notes.md
  - Project-planning and note-taking document made while working on this project.

---

## Justification of Database Schema
The star schema used in this database allows for faster analytical querying at the cost of consistency and storage costs (i.e., data is repeated in multiple places). By contrast, using a 3NF database here would require analytical queries to use many more joins and slow down the process of analysis.
With 'songplays' as the fact table, we are allowing greater querying power for analyzing session data specifically - which is what Sparkify needs in its analytical goals.

---

## Justification of ETL pipeline
The ETL pipeline I designed in order to limit S3 reading. The original template code suggested writing log data into S3 and then reading it back out in order to populate the 'songplays' table. My code instead *keeps* the log dataframe and passes it as an additional argument to create the songplays table - preventing an extra read from S3.

---