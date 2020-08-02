import psycopg2
import configparser
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read('ETL/config.cfg')


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname={} user={} password={}".format(*config['DB'].values()))
    cur = conn.cursor() 
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()