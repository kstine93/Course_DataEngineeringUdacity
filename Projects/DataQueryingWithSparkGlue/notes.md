# Project: Data Warehouse with Redshift

## Task

### Phase 1: Load & Clean
Using AWS Glue, S3, Python and Spark, generate Python scripts to build a Lakehouse solution in AWS for the STEDI project that satisfies the following requirements:
1. Data must be organized i- n S3 according to:
   1. Data 'cleanliness' (suggest: different buckets for different cleanliness? Or just different directories?)
   2. Data type (customer, accelerometer, etc.)
2. Glue tables must be created for landing zone data. **(deliverable: DDL SQL scripts (either generated or self-written)**
   1. `customer_landing.sql` (raw customer data table)
   2. `accelerometer_landing.sql` (raw accelerometer data table)
3. Query these Glue landing tables in Athena. **(deliverable: Provide *screenshots* of the resulting query + data in the web UI)**:
   1. `customer_landing.png`
   2. `accelerometer_landing.png`
4. Sanitize the customer data such that only customers who have consented to data storage and analysis are retained in a new `trusted_customer` S3 location *and* Glue Table
5. Sanitize the accelerometer data similarly and store in a new `trusted_accelerometer` S3 location and Glue Table
6. Query these cleaned Glue tables in Athena. **(deliverable: Provide *screenshots* of the resulting query + data in the web UI)**:
   1. `customer_trusted.png`
   2. `accelerometer_trusted.png`

### Phase 2:
Note from the data that customer identifiers repeat inappropriately in the 'customer' data set. These have been inappropriately assigned. However, you know that the data in the 'step_trainer' records table is correct. With this information, do the following:

1. Create a 'curated' data zone in S3 and a `customers_curated` Glue table that only includes customers who:
   1. Have agreed to share their data with us
   2. Have any accelerometer data
2. Create AWS Glue jobs to accomplish the following **(deliverable: Python scripts (either generated or self-written)**:
   1. Read in the step_trainer IoT data. Add this to a Glue table called `step_trainer_trusted`. This should only contain data from customers who have agreed to share their data for research.
   2. Create a combined table `machine_learning_curated` of accelerometer and step trainer data. This table should have the following features:
      1. Records should be matched on customer *and on timestamp* to ensure that the records are representing the same customer at the same time.
      2. The table should contain only records from customers who have agreed to share their data.

### Deliverables:
- [ ] A Python script using Spark that sanitizes the Customer data from the Website (Landing Zone) and only stores the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted. (**Phase 1.4**)
- [ ] A Python script using Spark that sanitizes the Accelerometer data from the Mobile App (Landing Zone) - and only stores Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted (**Phase 1.4**)
- [ ] A Python script using Spark that sanitizes the Customer data (Trusted Zone) and creates a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated. (**Phase 2.1)**
- [ ] A Python script using Spark that reads the Step Trainer IoT data stream (S3) and populates a Trusted Zone Glue Table called step_trainer_trusted containing the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated). (**Phase 2.2.1**)
- [ ] A Python script using Spark that creates an aggregated table that has each of the Step Trainer readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and populates a glue table called machine_learning_curated. (**Phase 2.2.2**)
- [ ] customer_landing.sql and your accelerometer_landing.sql script along with screenshots customer_landing (.png,.jpeg, etc.) and accelerometer_landing (.png,.jpeg, etc.) (**Phase 1.2 + 1.3**)
- [ ] A screenshot of your Athena query of the customers_trusted Glue table called customer_trusted(.png,.jpeg, etc.) (**Phase 1.6**)

---

## Data Formats


## Preliminary Notes

### Getting data into S3
The instructor is getting data into S3 by cloning a github repo into the AWS Cloudshell local storage, and then moving it from there to S3.
I don't really like this setup - it is manual and not easily scalable. How else could I get external data into S3?

- Get raw data files from GitHub; call CURL-like command in a script in Lambda to load these into S3 (note: if you append `?raw=True` onto the URLs for individual github files, you will be rerouted to a raw version of the file)
  - NOTE: I saw on a couple StackOverflow comments that this might cause latency issues because Github doesn't support this that well.
  - Maybe it's worth trying though - this is the only way beyond cloning the whole repo I think...
- An alternative might be to explore the Git REST API more, but I don't like this better since it requires an API key that needs storage - at this point might as well use the Cloudshell built-in git credentials.

**Note:** In the end, I decided to just `git clone` the files in the cloudshell as they did in the course - I don't really like this, but I can't think of an easier way to do it. If in the future I really wanted to be able to do this locally, I could [link a Linux instance with the Cloudshell](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/cloudshell-ssh.html).

```bash
#On AWS Cloudshell console, run the following commands:
#-------------------
git clone https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises.git

#Note that bucket must already exist, but subdirectories will be auto-created.
aws s3 cp nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer s3://glue-kstine-bucket-udacity/Project_GlueSpark/landing/accelerometer --recursive

#Checking transfer worked:
aws s3 ls s3://glue-kstine-bucket-udacity/Project_GlueSpark/accelerometer/landing/

```

---

## To-dos:

### Phase 1:
1. ~~Load all data (customer, step_trainer, accelerometer) from GH repo to S3 in `landing` directory. Format for all data should be all in 1 bucket (for convenience), but in different directories:~~
```
s3://stedi-data-lake
  | - landing
      | - customer
      | - accelerometer
      | - step_trainer
  | - trusted
      | - customer
      | - accelerometer
      | - step_trainer
  | - curated
      | - customer
      | - accelerometer
      | - step_trainer
```
2. ~~Load `landing` S3 data into Glue Tables (maybe just use Athena to take a quick looksie at the data, then define schema manually - rather than crawler).~~
   1. **NOTE:** In order to get the DDL in Athena, it was necessary to edit the Glue table to have the table property "TableType"="EXTERNAL_TABLE".
   2. ~~**DELIVER:** Once done, download the DDLs to my machine under `GlueTablesDDLs/landing`~~
3. ~~Query landing data in Athena (`SELECT * FROM {} LIMIT 5`)~~
   1.~~ **DELIVER:** For all table queries, take a screenshot of SQL and results and store under `AthenaQueries/landing`~~
4. ~~Create `landing_to_trusted_customer` data pipeline in Glue that takes `customer` data and scrubs out records from customers that have not consented to having their data stored (see lesson videos for exact criteria). This should result in a new S3 file **and** a new Glue Table.~~
   1. ~~**DELIVER:** Download generated Glue Spark code and store in `GlueETLCode` directory~~
5. ~~Create `landing_to_trusted_accelerometer` data pipeline in Glue that takes `accelerometer` data and scrubs out records from customers that have not consented to having their data stored (see lesson videos for exact criteria - maybe also check if a `JOIN` is the fastest way to do this...). This should result in a new S3 file **and** a new Glue Table.~~
   1. ~~**DELIVER:** Download generated Glue Spark code and store in `GlueETLCode` directory~~
6. ~~Query trusted data in Athena (`SELECT * FROM {} LIMIT 5`)~~
   1. ~~**DELIVER:** For all table queries, take a screenshot of SQL and results and store under `AthenaQueries/trusted`~~

### Phase 2:
7. ~~Create `trusted_to_curated_customer` data pipeline in Glue that takes trusted `customer` data and scrubs out records from customers that have no accelerometer data. This should result in a new S3 file **and** a new Glue Table.~~
   1. **NOTE:** The last exercise in the Spark + Data Lake module also asks to create a 'curated_customer' table, but their code results in ALL accelerometer data (400k records) rather than just filtering the customer data... This feels incorrect, and my code doesn't do this - but I should be prepared that I've missed something here...
   2. ~~**DELIVER:** Download generated Glue Spark code and store in `GlueETLCode` directory~~
8. ~~Create `landing_to_trusted_step_trainer` data pipeline in Glue that takes `step_trainer` data and scrubs out records from customers that have not consented to having their data stored (can be matched on email to customer). This should result in a new S3 file **and** a new Glue Table.~~
   1. ~~**DELIVER:** Download generated Glue Spark code and store in `GlueETLCode` directory~~
9.  ~~Create `to_curated_machine_learning` data pipeline in Glue that takes trusted `accelerometer` and `step_trainer` data matched on **customer** and on **timestamp** and produces a combined data set. This should result in a new S3 file **and** a new Glue Table.~~
   1. **DELIVER:** Download generated Glue Spark code and store in `GlueETLCode` directory

## Feedback from Reviewer of Project
(placeholder)