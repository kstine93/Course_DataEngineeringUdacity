# SQL Tips
There are some programming tips in Postgres that will be applicable more generally in database creation & manipulation. These are explained here.

## Keywords
### NOT NULL
When creating a table, you can specify which columns MUST have data using NOT NULL syntax

```
CREATE TABLE test_table (
    id int NOT NULL,
    value varchar
);
```
Note: If you have a composite key (multiple values combined represent the primary key), NOT NULL will be used for these components of the key.

### UNIQUE
Unique is used to specify that only unique values are allowed in a particular column.

```
CREATE TABLE test_table (
    id int UNIQUE,
    value varchar UNIQUE
)

#OR:

CREATE TABLE test_table (
    id int,
    value varchar,
    UNIQUE (id, value)
)
```

### PRIMARY KEY
Primary keys are by definition NOT NULL and UNIQUE. They designate the unique row identifier of the table.
Note: Specifying multiple columns to uniquely identify a row is a 'composite key'

```
CREATE TABLE test_table (
    id int,
    value varchar,
    PRIMARY KEY (id, value)
)
```

### ON CONFLICT
Particularly once data parameters (e.g., NOT NULL, UNIQUE) are specified, it's possible that requests could be rejected. We can use ON CONFLICT as a try/catch clause.
The following example is set up so that if a customer_id already exists with the value 421, then the customer's data is updated rather than a new row being inserted

```
INSERT INTO customer_address
    (customer_id shipping_address)
    VALUES
    (421,'123 Lewis Square')
ON CONFLICT (customer_id)
DO UPDATE
    SET shipping_address = EXCLUDED.shipping_address
```


- [More Postgres Documentation](https://www.postgresql.org/docs/9.4/ddl-constraints.html)