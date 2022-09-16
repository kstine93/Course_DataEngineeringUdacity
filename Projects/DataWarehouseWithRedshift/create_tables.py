import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    """Iteration to drop all tables in database

    Args:
        cur (psycopg2.cursor) cursor object from psycopg2; used to execute queries
        conn (psycopg2.connection) connection object from psycopg2; base connection to database

    Note:
        More info on Google-style Python docstrings:
        https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    """Iteration to CREATE all tables in database

    Args:
        cur (psycopg2.cursor) cursor object from psycopg2; used to execute queries
        conn (psycopg2.connection) connection object from psycopg2; base connection to database

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connects to database with configuration details from local 'dwh.cfg' file.
    Once connected, resets the database by DROPPING and then RECREATING all tables.

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()