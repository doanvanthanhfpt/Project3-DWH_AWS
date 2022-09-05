import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD, 5439))
    #conn = psycopg2.connect(host=HOST,dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
    #conn = psycopg2.connect("host=dwhHost dbname=dwh user=dwhuser password=Passw0rd port=5439")
    #conn = psycopg2.connect("host=redshift-cluster-3.cknnnnpyxyzp.us-west-2.redshift.amazonaws.com dbname=dwh user=dwhuser password=Passw0rd")
    
    #conn="postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, DWH_ENDPOINT, DB_PORT,DB_NAME)
    #print(conn_string)
    #%sql $conn_string

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()