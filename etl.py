import configparser
import psycopg2
from create_tables import validate_redshift
from sql_queries import copy_table_queries, insert_table_queries


config = configparser.ConfigParser()

# Read configuration values
config.read('dwh.cfg')


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Check that the redshift exists and get the connection
    conn = validate_redshift()
    cur = conn.cursor()
    
    print("Loading staging datasets . . .")
    load_staging_tables(cur, conn)

    print("Inserting data to final database . . .")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()