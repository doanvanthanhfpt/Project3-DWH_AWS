import psycopg2
from testQueries import create_table_queries, drop_table_queries


def connect_database():

    # connect to dwh database
    conn = psycopg2.connect("host=redshift-cluster-3.cknnnnpyxyzp.us-west-2.redshift.amazonaws.com dbname=dwh user=dwhuser password=Passw0rd port=5439")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
      
    return cur, conn


def drop_tables(cur, conn):

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():

    cur, conn = connect_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    #insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()