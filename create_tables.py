import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop the database tables they akready exist.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()

    # Read configuration values
    config.read('dwh.cfg')

    # Connect to the redshift
    connect = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cursor = connect.cursor()

    # For a flexible implementation as well as experiments, drop tables and then create tables
    # This approach ensures that we always reset the tables to test the ETL pipeline
    drop_tables(cursor, connect)
    create_tables(cursor, connect)

    connect.close()


if __name__ == "__main__":
    main()