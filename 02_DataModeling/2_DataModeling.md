# 02-Data Modeling
_Udacity - Data Engineering Nanodegree_

## Contents
- Differences between relational and Non-relational databases
- How to create relational data models using **Postgres SQL**
- How to create Non-relational data models using **Apache Cassandra**

### Database Management Systems (DBMS)
- *[Intro to DBMS](https://www.geeksforgeeks.org/introduction-of-dbms-database-management-system-set-1/)*
  - Notes:
    - DDL = "Data Definition Language", which is the language which deals in commands for altering the structure of the database
      - CREATE, ALTER, DROP, TRUNCATE, COMMENT, REANAME
    - DML = "Data Manipulation Language", which is the language which deals in commands for working with the actual data
      - SELECT, INSERT, UPDATE, DELETE, MERGE, CALL, EXPLAIN PLAN, LOCK TABLE

### ACID in database development
#### Atomicity
Transactions (e.g., writing, deleting) either completely **fail** or completely **succeed**. There is no possibility for only half of a job to succeed.
- This is helpful because it means that it is easier to track the status of the database. If a job fails, you know that nothing worked - and you can retry.

#### Consistency
Data has rules applied to it - the type, length, whether it's writable, etc. Whatever rules you apply should closely mimic the reality you are trying to represent (e.g., weight values should never be less than 0).
- By *appropriately* constraining the way data should behave in your database, you can enforce consistency- where any actions which would violate your rules are rejected.

#### Isolation
This involves database locking. Isolation ensures that two operations can NOT occur concurrently in a way which could produce an interaction effect (e.g., writing and deleting to a single table concurrently).
- The interaction effects could be difficult to predict depending on how the DBMS is implemented and what else happening at runtime on the machine. Running the same command twice could therefore produce different results on different machines, SQL implementations, or times if isolation is not enforced.
  
#### Durability
Changes made to the system are persistent - there are measures in place (e.g., backups) to ensure that data, once inputted or altered, is not lost arbitrarily.

## Data Modeling Overview
Data modeling is the process of working out how data will flow - and be stored - in a particular application. Like other sorts of planning, this usually involves first gathering requirements and conceptually mapping (e.g., making a diagram) of the data flows in the application.

`Note: it's helpful when data modeling to test your model with use cases (e.g., when a customer makes a purchase, how will data flow?). This can help ensure that your model is able to respond appropriately - that data is being written, read, and processed managably. `

**Terms**
- Conceptual Data Modeling = making your diagram of how data is stored & relates to itself
- Logical Data Modeling = Planning (on paper) how your conceptual model could be implemented in a general DBMS (think of schemas & exact relationships between tables/objects)
- Physical Data Modeling = Actually writing the code to implement your logical model in a DBMS

`Note: We can think about designing our data model to optimize for various use cases. A programmer might want to write to as few tables as possible for a particular transaction - to speed up the process. An analyst might also want all of their data in one place with as simple (or as adaptable) a query as possible. Also, we could try to think about how best to represent the data according to what real-world process we are actually trying to replicate / emulate (e.g., when setting up ecommerce, let's look at the order forms, warehouse, customers, etc. to see how this has been done non-digitally and see if we can replicate that digitally.`

## Relational vs. Non-relational Databases
### Relational DBs
- Organizes data into tables with columns and rows
- Unique keys identify each row
- a single row can be thought of as a Tuple (ordered set of data)
- Generally each table represents a SINGLE entity type (makes the RDBMS more scalable, generally)
- **Generally use SQL for accessing data and interacting with the database system**

**When to use RDBMS**
- Ability to combine data across multiple tables (joins)
- Ability to do aggregations & analytics
- Ability to do ad hoc queries allows you to change business requirements relatively easily (e.g., adding new tables, columns, querying something you didn't expect)
  - NoSQL doesn't allow just any type of querying - you have to know how you will query from the beginning.
- ACID is enforced, meaning that subsequent queries always have the most up-to-date information (NoSQL databases are consistent eventually, but prioritize speed over isolation)
  - **NOTE: MongoDB is one exception that can support ACID transactions**
    - MongoDB

### Non-relational DBs
- Non-relational databases are (typically) distributed databases. They can store information across multiple machines. This is in contrast to a RDBMS where all records have to 'live' under a single roof.
- Whereas RDBMS' can only add complexity / records by adding to a single existing machine, Non-relational databases can add more machines.
- Enforcing ACID is sub-otptimal for runtime. Non-relational doesn't enforce ACID, and so can run faster.
- Records can differ from one another in Non-relational (i.e., they can have different rows)

**Types of Non-relation (NoSQL) databases**
- Apache Cassandra
  - Data is still organized in rows + columns, but the tables are **partitioned** across multiple nodes.
- MongoDB
  - Data is organized as documents (independent files). Querying can search for titles of documents *as well as the contents of those documents.*
    - MongoDB's document structure essentially means that records are stored completely in one hierarchical JSON-like structure like this:
    ![image](media/mongodb_doc.png)
    - [MongoDB ACID White Paper](https://webassets.mongodb.com/MongoDB_Multi_Doc_Transactions.pdf?_ga=2.221758296.1324186602.1656439253-1203746384.1656439251)
- DynamoDB
  - "Key-value store"
    - So basically like a Dict/JSON?
- Apache HBase
  - "Wide Column Store"
  - Also uses tables, rows + columns, but the columns can vary between rows (kind of like independent entries)
- Neo4J
  - "Graph Database"
  - Somehow focuses on relationships between data (not sure what exactly this means yet)

### Introduction to Apache Cassandra
**Glossary**
- Keyspace = collection of tables
- Table = group of partitions
- Partition = Collection of rows
- Primary key = Partition key (which partition) and chosen uniquely-identifying "clustering column(s)"
- Row = a single 'item' (?)

**Helpful links**
- [Keys + Clustering Cols in Cassandra](https://www.bmc.com/blogs/cassandra-clustering-columns-partition-composite-key/)
  - Notes:
    - Cassandra sorts by clustering columns (for fast retrieval)
    - Cassandra is specifically optimized for *writing* data