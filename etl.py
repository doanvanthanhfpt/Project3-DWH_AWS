import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """ Load data from S3 to staging tables """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """ Load data from staging tables to fact and dimension tables """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    """ Connect Redshift cluster database and get db cursor"""
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # for test: #conn = psycopg2.connect("host=redshift-cluster-3.cknnnnpyxyzp.us-west-2.redshift.amazonaws.com dbname=dwh user=dwhuser password=Passw0rd port=5439")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()