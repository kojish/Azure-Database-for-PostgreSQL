 # Licensed under the MIT license. See LICENSE file in the project root for full license information.
import psycopg2
from psycopg2 import pool
import datetime
import time
import random
from retrying import retry

# Update connection string information obtained from Azure portal
host = "yourhost.postgres.database.azure.com"
user = "user@xxx"
dbname = "databasename"
password = "password"
sslmode = "require"

# Construct connection string
conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

# Retry parameters
max_attempt_num = 10
exp_multipler = 1000
exp_max = 60000

# 0 if connection is open, nonzero if it is closed or broken
CONNECTION_OPEN = 0

#
# Connect to PostgreSQL with specified retry parameters
#
@retry(stop_max_attempt_number=max_attempt_num, wait_exponential_multiplier=exp_multipler, wait_exponential_max=exp_max)
def retryable_connect(start_time):
    print("Re-connecting to PostgreSQL")
    print('Time elapsed after connection failure: {:.2f} sec'.format(time.time() - start_time))
    return connect(start_time)

#
# Create a connection with a connection pool to PostgreSQL.
# Returns connection object.
#
def connect(start_time):

    pool = None
    try:
        pool = psycopg2.pool.SimpleConnectionPool(1, 20, conn_string) 
        if(pool):
            print("Connection pool created successfully.")
            conn = pool.getconn()

        if(conn):
            print("Obtained connection from the pool successfully. ", conn.closed)
            print(conn.get_dsn_parameters(),"\n")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL: ", error)
        if pool is not None:
            pool.closeall

    return conn


def insert_query():
        # cursor.execute("SELECT version();")
        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        timestamp = dt + '+00'
        # Insert some data into table
        ret = "INSERT INTO readings VALUES ('%s', 'demo000001', '55', 'discharging', 89.9, '01:03:02:04:05:06', 8.06, 6.32358599064555, 10.1801563850155, 409704427, 590295573, -49, 'demo-net');" % timestamp
        return ret

# Obtain query string
def get_query_string():
    #return "SELECT * FROM readings LIMIT 1;"
    return insert_query()

# main function
def main():

    conn = connect(time.time())
    statement = get_query_string()
    print(statement)

    while True:
        try:
            if conn.closed != CONNECTION_OPEN:
                print("Connection is failed. Wait for 5 sec before first retry....")
                time.sleep(5)
                conn = retryable_connect(time.time())

            while True:
                with conn.cursor() as cursor:
                    cursor.execute(statement)
                    conn.commit()
                    time.sleep(5)

                #print("Completed SQL query: ", cursor.query)
                print("Completed SQL query.")

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting or querying to PostgreSQL: ", error)

        finally:
            if(conn):
                conn.commit()
                cursor.close()
                conn.close()


if __name__ == '__main__':
    main()
