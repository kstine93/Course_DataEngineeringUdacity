
#psycopg2 is a Postgres "adapter" for Python
#https://pypi.org/project/psycopg2/
import psycopg2
from psycopg2 import sql


#==SETUP==#
host = "127.0.0.1"
dbname = "practice_db"
user = "basic_user"
password = "password"
#Note: The values above (besides host) needed to be specified in Postgres
#I did this via Postgres CLI (see accompanying 'linux_postgres_setup.md' file)

#Note: try/except clauses are nice for error handling. I think it's too much to have them for every
#command, so I'm only putting a few here as examples (cool that we can specify errors from psycopg2)
try:
    conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
except psycopg2.Error as e:
    print("Database connection failed.")
    print(e)
    
'''Note: Postgres (and presumably other SQL implementations) attempt to maintain ISOLATION
This can manifest that after every command on the same table (if a write command), we need
to COMMIT OUR CHANGES. It's a bit like using Git - you can make changes, but they're not
really implemented until you commit them.
You will see this error as "cannot run inside transaction block"

Additionally, if we don't like the changes we've made, we can roll them back
using 'rollback'
'''
conn.rollback() 
conn.commit() #This makes all of the changes above permanent in Postgres

#Alternatively, we can auto-commit; which means that 'commit' is automatically
#called after every query.
conn.set_session(autocommit=True)

try:
    cursor = conn.cursor()
except psycopg2.Error as e:
    print("Database cursor setup failed.")
    print(e)

new_db = "practice_db2"

try:
    query = sql.SQL("DROP DATABASE {db_name}").format(
        db_name=sql.Identifier(new_db)
    )
    cursor.execute(query)
except psycopg2.Error as e:
    print("Database creation failed.")
    print(e)

try:
    query = sql.SQL("CREATE DATABASE {db_name}").format(
        db_name=sql.Identifier(new_db)
    )
    cursor.execute(query)
except psycopg2.Error as e:
    print("Database creation failed.")
    print(e)

#When we want to switch databases, we need to recreate the connection:
try:
    conn = psycopg2.connect(f"host={host} dbname={new_db} user={user} password={password}")
    cursor = conn.cursor()
except psycopg2.Error as e:
    print("Database connection or cursor initialization failed.")
    print(e)
    

#Important note: psycopg documentation notes that Python's dynamic string interpretation can sometimes fail
#with postgres, since some characters (e.g., apostrophes) can be interpreted differently.
#Therefore, the BEST way to write dynamic queries is to let the 'execute' command interpret for you
#(see examples below)
#https://www.psycopg.org/docs/usage.html

#==CREATE TABLE==#
table_name = "test_table"
try:
    query = sql.SQL("CREATE TABLE {table} ({field1_name} {field1_type}, dob date, id UUID DEFAULT gen_random_uuid() PRIMARY KEY);").format(
        table=sql.Identifier(table_name),
        field1_name=sql.Identifier("name"),
        field1_type=sql.Identifier("varchar")
    )
    cursor.execute(query)
    del query
except psycopg2.Error as e:
    print(e)

#==INSERTING DATA==#
try:
    #Annoyingly, I got errors when trying to use the 'sql.identifier syntax here.
    #The course instructors got normal string interpolation to work, so doing that here:
    query = "INSERT INTO test_table (name, dob) VALUES (%s, %s);"
    values = ("kevin","2000-08-24")
    cursor.execute(query,values)
    del query, values
except psycopg2.Error as e:
    print(e)
#==QUERYING==#
cursor.execute(f"SELECT * FROM {table_name}")
print(cursor.fetchall())

#==EDITING DATABASE==#
cursor.execute(f"DROP TABLE {table_name}")

conn.close()
