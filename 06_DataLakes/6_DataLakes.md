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
