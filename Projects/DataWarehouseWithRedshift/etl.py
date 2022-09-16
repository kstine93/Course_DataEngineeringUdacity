import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    """Iteration to move data from S3 bucket to intermediate tables

    Args:
        cur (psycopg2.cursor) cursor object from psycopg2; used to execute queries
        conn (psycopg2.connection) connection object from psycopg2; base connection to database

    Note:
        More info on Google-style Python docstrings:
        https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
    
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    """Iteration to INSERT data from intermediate tables into production tables

    Args:
        cur (psycopg2.cursor) cursor object from psycopg2; used to execute queries
        conn (psycopg2.connection) connection object from psycopg2; base connection to database

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connects to database with configuration details from local 'dwh.cfg' file.
    Once connected, calls subfunctions to load data from S3 into intermediate tables,
    and to then move data from those intermediate tables into the production tables.
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()