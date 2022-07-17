# Project: Data Modeling with Cassandra

## Task
- Client wants to query `what songs users are listening to`
  - I need to create a database to serve these queries; **client has provided specific queries they intend to use**
- Flow: CSV files --> ETL.py script --> Cassandra database

## Data Formats
- CSV files
  - Separate files represent separate DAYS
    - *Note: might be a good idea to partition on days anyway - possible to take advantage of the fact that partitions and files are similarly divided (e.g., processing data in batches)?*
    - 
### Desired Schema

## Preliminary Notes

## To-dos:
1. Design tables
2. Create Cassandra keyspace + tables
   1. Use `IF NOT EXISTS` to avoid re-creating tables
      1. Why?
3. Design INSERT statements
   1. Also need ON CONFLICT? If yes, consider what policy would make sense
4. Build ETL to iterate over CSV files and
   1. Clean / transform data (if needed)
   2. load into Cassandra tables
5. Go back to original CSV file transformation and try to optimize (see my markdown notes there)
