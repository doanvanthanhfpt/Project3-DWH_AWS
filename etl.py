import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
""" Import necessaries Python modules """

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
""" Load data from S3 to staging tables """

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
""" Load data from staging tables to fact and dimension tables """

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host=redshift-cluster-3.cknnnnpyxyzp.us-west-2.redshift.amazonaws.com dbname=dwh user=dwhuser password=Passw0rd port=5439")
    cur = conn.cursor()
    """ Connect Redshift cluster database and get db cursor"""
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()