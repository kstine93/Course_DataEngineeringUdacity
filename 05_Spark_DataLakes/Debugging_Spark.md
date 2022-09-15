# Debugging Spark
_Udacity - Data Engineering Nanodegree_

## Why debugging Spark is hard
1. Spark is running on a set of other machines - that might be configured differently than your computer. Even if your code runs in local mode, it might fail in the different environment.
2. You don't have direct access to the Spark logs on each machine in the cluster. Instead, they are all captured on a single that you must then get the logs from.
3. Spark uses **lazy evaluation** - meaning that a mistake made previously in the code might not throw an error until a later step.
4. Spark is written in Scala, so errors might refer to Scala, Java or JVM issues even though code might be written in Python.

## Some common Spark bugs
- Typos in method names (produce short 'attribute error's)
- Typos in column names for a data frame (produce long 'analysis exception error's)
- Using `collect()` with too much data
- Mismatched parentheses & typos in variables (the usual suspects)
- using Python aggregate functions (e.g., "sum") instead of the Spark equivalent (also called "sum" - needs to be given an alias)

## Spark Accumulators as debugging help
"Accumulators are like global variables for your entire cluster".

Since Spark is running in a distributed manner, we can't introduce any state dependencies between worker nodes that we might use for debugging, but we *can* ask our worker nodes to append data to an accumulator.

> EXAMPLE:
> Let's say we are cleaning our data set and we want to count how many rows have an incorrect value for a certain column and print out that number. With a small(ish) data set, we could simply do this in one command in a Jupyter notebook, but that won't work for big data.
> Instead, in this case, we can use an accumulator to count how many rows are found by each worker node and sum the result:

```
#Defining an accumulator
records_count = SparkContext.accumulator(0,0)

#Function to increment accumulator
def increment_records_count():
    global records_count
    records_count += 1

#Function to find rows matching criteria and increment counter:
from pyspark.sql.functions import udf
check_record = udf(lambda x: increment_records_count() if x == "Attack Helicopter" else 1)

#Add column showing result:
df = df.withColumn(good_record,check_record(df.gender))

#Force Spark to run:
df.collect()

#Check result:
records_count.value
```

**NOTE:** As with all global variables, accumulators are vulnerable to errors when their values are not reset, but code is repeated for some reason.

**NOTE:** I like the idea much more of creating an output data frame which analyses all of my columns and provides some statistics (e.g., how many values in a specified column matched user-given criteria? How many missing values per column?). Then I could take that output data frame and get lots of answers instead of setting up lots of accumulators.


[Spark documentation on Accumulators](https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#accumulators)

---

