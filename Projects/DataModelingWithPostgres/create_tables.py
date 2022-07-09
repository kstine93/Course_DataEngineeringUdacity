import psycopg2
from psycopg2 import sql
from sql_queries import create_table_queries, generic_table_drop, tables

#Database
def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    #Terminating connections to database in need of re-creation:
    cur.execute('''SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = 'sparkifydb';''')
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn

#Table
def create_all_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def drop_all_tables(cur, conn):
    
    for tbl in tables:
        query = sql.SQL(generic_table_drop).format(
            table=sql.Identifier(tbl)
        )
        cur.execute(query)
    #Committing only after all queries execute successfully (atomicity)
    conn.commit()
    
def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur,conn = create_database()
    
    drop_all_tables(cur, conn)
    create_all_tables(cur, conn)

    #end_postgres_session(conn)


if __name__ == "__main__":
    main()