# 06- Data Lakes
_Udacity - Data Engineering Nanodegree_

## Data Lakes - Why?
Data Lakes were born initially of data warehousing technology (and instructor notes that data warehouses are still the best solution for many organizations today).

However, data warehouses are not always the most adept at specific problems that occur more often nowadays including:
- abundance of non-tabular data (images, xml, json, voice, PDF, etc.)
  - While it is possible to put JSON or XML into some kind of tabular format, we might not want to depending on how that data is eventually used. Additionally, deep JSON or XML structures would be increasingly hard to store in a tabular way.
- huge increase in data volume (resulting from IoT, etc.)
- rise of big data technologies like HDFS & Spark
  - HDFS made it possible to store petabytes of data on commodity hardware, incurring much less cost per TB vs. typical databases.
  - MapReduce, Pig, Hive, Impala, Spark, etc. made it possible to process data at scale alongside storage of the data.
  - Additionally, these tools made it possible to analyse data dynamically without needing to first create traditional tables (e.g., read.csv + create df + analyse data)
- new types of data analysis, like ML, that sometimes require more flexible formats of data (e.g., Natural Language Processing)

---

## Impacts of Big Data on infrastructure:
- Once big data infrastructure (e.g., Spark, HDFS) started to become more popular, **ETL offloading** became a advantageous choice.
>ETL Offloading: Using the same hardware for storage AND processing (i.e., nodes of a computer cluster)
- Decreasing costs for storage of data allowed for storage of more low-value or unknown-value data not previously available for analytics.
  - e.g., Data Lakes do not always require the explicit structuring of data, so including plain-text or unusually-formatted data became easy to do without significant investment.
>Schema on Read: When the schema of a file is either inferred or specified, but the data is NOT inserted into a database. Regardless, these data can still be represented tabularly and queried as if they were part of a database.

**Note:** With Schema on Read, we can easily pull in data files and query them. However, the *inferred* schema is not always correct (e.g., assuming date is a string rather than timestamp). For production or long-term data structures, you should specify the schema:
```
df_schema = StructType([
  StructField("id",IntegerType()),
  StructField("cost", DoubleType()),
  StructField("date", DateType())
])

df = spark.read.csv("myFile.csv",
  schema = df_schema,
  sep=";",
  mode="DROPMALFORMED"
)

#Registering my df as a table so I can run SQL on it:
df.createOrReplaceTempView('df_view')
spark.sql('SELECT * FROM df_view')
```

**Data Formats we can use with Big Data tools:**
- CSV
- JSON
- XML
- plain text
- Avro (binary; saves space)
- Parquet (columnar; limits reading of unneeded data)
- compressed formats (e.g., gzip, snappy)

---

## Architecture of Data Lakes

The biggest difference between Data Lakes and Data Warehouses is that **in data lakes, data is NOT pre-processed and standardized.** Rather, it is all simply loaded and stored- and then is only processed afterwards.

In other words, **Data Lakes are ELT, not ETL**.

### More differences between Data Lakes & Warehouses:

<img src="./media/DataLakeVsWarehouse.png" width=800px>


---

## Data Lake Implementation Options on AWS

---

#### EMR with HDFS + Spark

We spin up an EMR cluster where each of our nodes is running HDFS and Spark. Each of these nodes function as both storage and processing. These nodes store the data until it is needed by downstream applications, at which point the clusters process the data (question: interactively? Does data-requester specify what processing should be done?) and pass it off to downstream apps.

**NOTE:** I don't really understand why we would want this architecture. All of our nodes are glorified storage disks until they need to do processing - so their CPUs and memory are not used at all most of the time. Additionally, if Data Lakes are ELT (with transforming happening last), I don't see why we shouldn't pass the cost of transformation to the requester. There's no need to have centralized processing if there is no standardization. Rather the requester can know what data they need, how they need to transform it, and can set up their own transformation pipeline custom to their need.

---

#### EMR with S3 + Spark

We ingest directly to S3- which is effectively our Data Lake. Then, we can offer EMR (or Databricks on EC2, for example) for others to use in pulling, transforming, and re-saving data to S3.

**NOTE:** I like this. It allows us to grow processing independently of storage (and vice-versa). It also allows the flexibility to process data elsewhere if desired (e.g., I could download data to my local machine from S3 and run a local transformation program)

---

#### Amazon Athena with S3

We ingest directly to S3- which is effectively our Data Lake. Then, we can offer Amazon Athena for others to use in pulling, transforming, and re-saving data to S3.

Amazon Athena is **essentially a cluster of Lambda instances**. Lambdas can be more cost-effective than EC2 (and certainly more cost-effective than EMR) since you pay by runtime of the code you input rather than the uptime of an entire machine.

**NOTE:** What might be downsides of this approach vs. EMR (or Databricks on EC2)?
- Maybe Athena is less configurable? (can you run Spark on Athena for big data jobs?)
- What's the difference in configurability between Athena and Databricks (as 2 cluster and code management tools)

