## Installing Postgres Pagila database
The Pagila database is a sample database that Postgres offers.
You can download the files holding the data [here](https://www.postgresql.org/ftp/projects/pgFoundry/dbsamples/pagila/)

Once you've downloaded the data, you will see a few files:
- pagila-data.sql
- pagila-insert-data.sql
- pagila-schema.sql
- README

Using the command line on Linux, you can ask Postgres to load in this database for you to use.

### Instructions
#### 1. Put pagila files in '/tmp' folder
Postgres will not have permission to read files from most of your folder structure in Linux. However, the 'tmp' folder is by default publicly accessible.

---

#### 2. Log into Postgres, and then access Postgres terminal
```
> sudo -i -u postgres
> psql
```

---

#### 3. Create database where you want the data to be stored
```
> CREATE DATABASE pagila;
```

---

#### 4. Load in Schema, then insert-data file
```
> \i /tmp/pagila-0.10.1/pagila-schema.sql
> \i /tmp/pagila-0.10.1/pagila-insert-data.sql
```
---

#### 5. Query data
You can then switch over to the postgres database and look at the tables
```
> \c postgres
> \dt
> SELECT * FROM actor;
```