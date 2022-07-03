-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = { 'class' : 'SimpleStrategy
, 'replication_factor':'1'};

-- Create a table
CREATE TABLE IF NOT EXISTS store.cart (
    userid text PRIMARY KEY,
    quantity int,
    last_update timestamp
);

-- Insert data
INSERT INTO store.cart (userid, quantity, last_update) VALUES ('0001',2,toTimeStamp(now()));
INSERT INTO store.cart (userid, quantity, last_update) VALUES ('0002',4,toTimeStamp(now()));