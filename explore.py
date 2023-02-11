import configparser
from create_tables import validate_redshift

config = configparser.ConfigParser()

# Read configuration values
config.read('dwh.cfg')

def num_of_staging_events(conn, cur):
    query = ("""
        SELECT COUNT(*) AS rows FROM staging_events_table
    """)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    
    print("SELECT COUNT (*) FROM staging_events")
    n_row = cur.fetchone()
    print(n_row)

def num_of_staging_songs(conn, cur):
    query = ("""
        SELECT COUNT(*) AS rows FROM staging_songs_table
    """)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    
    print("SELECT COUNT (*) FROM staging_songs")
    n_row = cur.fetchone()
    print(n_row)

def num_of_songplays(conn, cur):
    query = ("""
        SELECT COUNT(*) AS rows FROM songplay_table
    """)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    
    print("SELECT COUNT (*) FROM songplays")
    n_row = cur.fetchone()
    print(n_row)

def num_of_users(conn, cur):
    query = ("""
        SELECT COUNT(*) AS rows FROM user_table
    """)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    
    print("SELECT COUNT (*) FROM user_table")
    n_row = cur.fetchone()
    print(n_row)

def num_of_songs(conn, cur):
    query = ("""
        SELECT COUNT(*) AS rows FROM song_table
    """)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    
    print("SELECT COUNT (*) FROM song_table")
    n_row = cur.fetchone()
    print(n_row)

def num_of_artists(conn, cur):
    query = ("""
        SELECT COUNT(*) AS rows FROM artist_table
    """)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    
    print("SELECT COUNT (*) FROM artist_table")
    n_row = cur.fetchone()
    print(n_row)

def num_of_times(conn, cur):
    query = ("""
        SELECT COUNT(*) FROM time_table
    """)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    
    print("SELECT COUNT (*) FROM time_table")
    n_row = cur.fetchone()
    print(n_row)

def most_played_songs(conn, cur):
    query = ("""
        SELECT title, COUNT(songplay_id) 
        FROM song_table s
        JOIN songplay_table p ON s.song_id = p.song_id
        GROUP BY title
        LIMIT 6
    """)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    
    print("Most played song")
    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()

def most_played(conn, cur):
    query = ("""
        SELECT title, COUNT(songplay_id) 
        FROM song_table s
        JOIN songplay_table p ON s.song_id = p.song_id
        GROUP BY title
        LIMIT 6
    """)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    
    print("Most played song")
    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()


def main():
    # Check that the redshift exists and get the connection
    conn = validate_redshift()
    cur = conn.cursor()

    num_of_staging_events(conn, cur)
    num_of_staging_songs(conn, cur)
    num_of_songplays(conn, cur)
    num_of_users(conn, cur)
    num_of_songs(conn, cur)
    num_of_artists(conn, cur)
    num_of_times(conn, cur)
    most_played_songs(conn, cur)

    conn.close()


if __name__ == "__main__":
    main()