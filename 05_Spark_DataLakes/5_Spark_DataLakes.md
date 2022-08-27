# 05-Spark & Data Lakes
_Udacity - Data Engineering Nanodegree_

## What is Big Data?
- No single definition, but a rule of thumb: big data is not possible to process on a single machine

---

## Review of Hardware behind Big Data
There are a lot of performance indicators that can be measured on a computer. Below show some indicators for a typical commercial computer around 2020:
<img src = "./media/2020ComputerSpecs.png" width=600px />

Four of the most basic - and important - ones are:
- CPU operation (time to complete task)
- Memory Reference (time to read / write to CPU)
- Storage (time to read / write to memory or CPU)
- Network (time to send / receive a message to another computer)

>The CPU is **200x faster** than memory

>Memory is **15x / 200x faster** than SSDs / HDDs

>SSDs are **20x faster** than the network

**Question:** These numbers are suggesting that actually a *centralized* system would be most performant - if we can keep most of our data in memory and outside of storage and especially a network, we'll have faster processing. How then do we decide to use a distributed system instead? By optimizing the network somehow?

---

## Introduction to Distributed Systems
One reason that distributed systems have become so popular is because:
- They can be cheaper to build than a centralized system
  - If we compare a distributed system to a single machine, even if we kitted out that single machine with dense RAM sticks and many cores, that could become much more expensive to build custom than simply linking together regular computers and distributing the tasks across them
- They can be more scalable
  - If you limit yourself to a single machine, you will reach a point where it is no longer possible to scale the machine any more. However, a distributed system can scale virtually limitlessly, as long as the network can keep up.

Speaking of network, the instructor noted that while CPU speed, memory density, and storage capacity have all doubled every few years, the speed of the network has lagged behind - and is still quite slow when compared to these other processes.

However, there are some ways that distributed computing systems (like Spark) try to avoid excess network traffic, including:
- Limiting **Shuffling** - moving data back-and-forth in between distributed nodes of a cluster (e.g., like when Redshift doesn't have all of its data for a query within a single node)

---

## When to use centralized vs. distributed system

Using the example of Sparkify music database: let's imagine that Sparkify is brand new - and we only have ~4GB of log data to analyze.

In this case, it's relatively easy to put all of that data in the RAM of your machine and run your Python script to analyze it.
In contrast, you could instead split that 4GB data and send it out to 6 other machines to process, but **this will take longer in the end**. Your computer as-is has the power to handle this data. By distributing the processing, you include the network, which will slow things down a ton. Additionally, including other machines has the problem of making sure they're all set up correctly, not crashing, available, etc.

Now, what happens when a year or so later we have *200GB* of data to process?
Well, your computer will go *really slowly* or it will crash.
This is where we need to get more strategic about how we process this data. A few options:
- We chunk the data into smaller pieces and process them sequentially
  - e.g., if we are working with a sorted data set, we could split the file in 2 (or more) parts such that we could perform operations on each part separately and then simply concatenate them in the end (e.g., if summing up counts of a certain value)
- We process in a distributed way - sending out small bits of work to other machines
- We find a way to cut out irrelevant data from the file *before* processing (e.g., parquet files allow you to only grab certain columns of data)

The moral of the story is that **distributed processing makes sense once your computer alone can no longer comfortably process the data itself.**

---

## Parallel Computing vs. Distributed Computing
Distributed computing and parallel computing are very similar in that we are pooling resources to handle a certain task.

However, in **distributed computing**, each 'node' has its own private CPU(s) and memory. All communication happens over a network *between* nodes.

In contrast, **parallel computing**, CPUs share resources (usually the same memory), which makes communication between CPUs very fast. It's more a way of distributing work *within* a machine rather than outsourcing the work.

- Note: there is a LOT of literature on frameworks (like [OpenCL](https://leonardoaraujosantos.gitbook.io/opencl/performance)) which allow you to parallelize processing on a machine. Might be worth looking into more.
---

## History of Spark

### Hadoop
Spark was predated by **Hadoop** - an ecosystem for big data storage and analysis. The major difference between Hadoop and Spark is how they use memory. Hadoop writes intermediate results to disk, while Spark keeps them in memory whenever possible - making Spark faster for many use cases.

**Hadoop MapReduce** is a system for processing and analyzing large data sets in parallel

**Hadoop YARN** is a resource manager that schedules jobs across a cluster. The manager keeps track of what resources are available and assigns them to specific tasks

**Hadoop Distributed File System (HDFS)** is a big data storage system that splits data into chunks and stores them across clusters. **Note that Spark does not have its own file system**

As Hadoop grew, other tools were developed to make Hadoop easier to work with, including:

**Apache Pig** - a SQL-like language that runs on top of MapReduce
**Apache Hive** - *another* SQL-like interface that runs on top of MapReduce

>Note: When people talk about "Hadoop", they are generally referring to the MapReduce part of Hadoop.

---

### Hadoop MapReduce
Example:
If we want to analyse a large dataset with Hadoop, we would want to use MapReduce.
The first 'preparation' step of using MapReduce is **partitioning** wherein a large data set is 'chunked' into smaller pieces.
Then, the data goes through 3 steps in MapReduce:
- Map
  - The partitions are given out to separate machines which all perform the same operation on the partition (e.g., filtering). Results of the map are written to an intermediate file.
- Shuffle
  - Let's imagine that our intermediate results now need to be aggregated somehow. Maybe in our map step we extracted some data from a bigger table, and now we want to aggregate similar records. In the shuffle step, we will re-arrange data across nodes to *prepare* for this aggregation in the 'reduce' step
- Reduce
  - Reduce performs the final aggregations on the data - so with the shuffled records from the previous step, we can count, or countUnique, or Sum, etc. Results are then returned to the user.


---

### From Hadoop to Spark
Spark is a big data framework similar to Hadoop. Spark contains libraries for data analysis, machine learning, graphing, and streaming live data.
Spark is generally *faster* than Hadoop

---

## Common Spark Use Cases
Spark is a big data framework similar to Hadoop. Spark contains libraries for data analysis, machine learning, graphing, and streaming live data.

Spark does not have a native file system, but it does allow ingestion directly from sources like S3.

Additionally, Spark has a data streaming library called [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).

> There are other streaming libraries available too, including [Apache Flink](https://flink.apache.org/flink-architecture.html)

> Also see [this Medium article](https://medium.com/@chandanbaranwal/spark-streaming-vs-flink-vs-storm-vs-kafka-streams-vs-samza-choose-your-stream-processing-91ea3f04675b) for a discussion of some of these tools and the tradeoffs.

---
 
## Spark Infrastructure
Spark - and many other distributed computational systems - use a Manager-Worker hierarchy, where a particular node is responsible for orchestrating the work of all other nodes.

Spark offers 3 different types of 'Managers' that you can work with:
- Spark's native Standalone Cluster Manager
- YARN - from the Hadoop framework
- Mesos - open source manager from UC Berkeley

YARN and Mesos are useful when you are sharing a cluster with a team (apparently), but they won't be covered in this course.
>To Do: Check out YARN - heard about it at work; probably is pretty relevant.

Additionally, Spark offers a "local" mode of working which allows you to use Spark APIs normally, but all operations are performed on your machine (useful for testing, prototyping)

---

---

# Data Wrangling with Spark

## Functional Programming
Functional programming gets its name from Algebra - in that a function, given the same input, will always give the same output (no global variables or interactions outside the function).

Functional programming avoids **shared state, mutable data, and side effects**. Each function will not be affected by the state of the rest of the system at a particular time.

Shared state and mutable data mean that the outcome of a particular task is dependent on state of the environment in which it runs. To know how the function works, you need to understand the entire environment and its history.

**Race Conditions**
Additionally, shared state can introduce **race conditions** - which essentially mean that the correct outcome of a task is dependent on time- and if one process happens to be faster than another, the task can fail.

**Separation**
Another tenet of functional programming is separation - which is the concept of performing logic separately from creating effects. It's a bit similar to mise en place in the kitchen: you should perform all of your logic first, and then if the logic is successful, perform actions. This can help prevent partial effects from creeping in when your logic fails.

**Conservation**
Create general, simple functions which can be re-used. Build complex programs out of smaller, atomic pieces.

---

### Maps in functional programming
The term 'Map' comes from the mathematics concept of 'mapping' inputs to outputs.
Map functions make a **copy** of the original data and perform functions on it before returning it.

---

### Why does Spark use a functional language?
Spark is written in **Scala** - which is a **functional** programming language, but there are APIs which allow you to use Spark with Java, R, and Python (the API for using Spark in Python is called **PySpark**).

In this way, functional programming limits the errors that can cripple distributed systems. For example, in distributed systems, it's common for one machine to need to re-start and re-do some calculations. However, when that machine depends on a shared state with all other machines, it means that a single machine going down has now caused complications for ALL machines.

---

## Directed Acyclic Graphs (DAGs) & Lazy Evaluation in Spark
Before Spark evaluates any part of your program, it creates a DAG in which it maps out the flow of your program and when it will need certain data.

This DAG allows Spark to determine where in your program it can **delay certain processes** (e.g., loading data into memory). Then it creates an execution plan which only performs certain processes *at the last possible moment*. This is **Lazy Evaluation** and it helps prevent Spark from having to maintain certain processes before their needed (which could lead to network timeout, out-of-memory errors, etc.)

---

## Starting with Spark
The first component of each Spark Program is the **Spark Context**. The Spark context connects the cluster with an application. There are ready-made spark contexts to use, but you can specify custom contexts as well:

```
from pyspark import SparkContext, SparkConf

#Note: If we run Spark in local mode, we can put the string "local" in lieu of an IP address below
config = SparkConf().setAppName("name").setMaster("IP Address")

sc = SparkContext(config)


```


---

### Common Data formats in Big Data
**CSV**
Comma-separated value files stores data in row-based tables, where each row represents a record.
However, there is no agreed-upon standardization for CSV files, so edge cases (like where values contain commas or newline characters) can be handled differently by different programs.

**JSON**
Javascript object notation files store data in key-value pairs. Seen a lot in headers for HTTP requests, for example.

**HTML**
Hypertext markup language files contain a huge amount of unique language for defining formatting for text and images to be rendered as a 'page' by a webbrowser.

**XML**
Extensible markup language is a "generalized" version of HTML where the tags do not have an agreed-upon meaning. Rather, you can define your own tags.

---

### Distributed data storage
Spark itself does not offer any data storage capabilities, but it can integrate with both Hadoop Distributed File System (HDFS) and Amazon S3.
>Note: HDFS works by splitting data into 64 or 128-MB blocks and replicating them multiple times across the cluster (to ensure fault-tolerance: if a cluster node fails, data is not lost).

---

## Spark Environment & APIs

---

## RDDs

---
