import configparser

# CONFIGURATION
config = configparser.ConfigParser()
config.read('dwh.cfg')

# s3 = boto3.resource('s3',
#                        region_name="us-west-2",
#                        aws_access_key_id=KEY,
#                        aws_secret_access_key=SECRET
#                    )

# sampleDbBucket =  s3.Bucket("udacity-dend")
# for obj in sampleDbBucket.objects.filter(Prefix="song_data"):
#     print(obj)

# DROP TABLES QUERIES
staging_events_table_drop = "DROP TABEL staging_events_table"
staging_songs_table_drop = "DROP TABEL staging_events_table"
songplay_table_drop = "DROP TABEL staging_events_table"
user_table_drop = "DROP TABEL staging_events_table"
song_table_drop = "DROP TABLE song_table"
artist_table_drop = "DROP TABLE artist_table"
time_table_drop = "DROP TABLE time_table"

# CREATE STAGING TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events_table 
        (
            id IDENTITY(0, 1) NOT NULL PRIMARY KEY,
            artist text, 
            auth text,
            firstName varchar(100),
            gender varchar(10),
            itemInSession int,
            lastName varchar(100),
            length float,
            level varchar(5),
            location text,
            method varchar(),
            page varchar(20),
            registration double,
            sessionId int,
            song text,
            status int,
            ts timestamp,
            userAgent text,
            userId int
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs _table
    (
      id IDENTITY(0, 1) NOT NULL PRIMARY KEY,
      num_songs int,
      artist_id text,
      artist_latitude float,
      artist_longitude float,
      artist_location text,
      artist_name varchar(100),
      song_id int,
      title text,
      duration float,
      year int

    )
""")

# CREATE FACT - song play
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table 
        (
            songplay_id IDENTITY(0, 1) NOT NULL PRIMARY KEY,
            start_time timestamp, 
            user_id int, 
            level varchar(50), 
            song_id INT, 
            artist_id INT, 
            session_id INT, 
            location text, 
            user_agent text
        )
""")

# CREATE DIMENSIONAL TABLES - user, song, artist, and time tables
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table
    (
        user_id IDENTITY(0, 1) NOT NULL PRIMARY KEY, 
        first_name varchar(100), 
        last_name varchar(100), 
        gender varchar(10), 
        level varchar(5)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs_table
    (
        song_id IDENTITY(0, 1) NOT NULL PRIMARY KEY, 
        title text, 
        artist_id int, 
        year int, 
        duration float
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_table
    (
        artist_id IDENTITY(0, 1), 
        name text, 
        location text, 
        lattitude float, 
        longitude float
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table
    (
        time_id IDENTITY(0, 1),
        start_time timestamp, 
        hour int, 
        day int,
        week int, 
        month int, 
        year int, 
        weekday boolean
    )
""")

# STAGING TABLES - Load data from S3 and stage them in Redshift databases

# Using auto option since the event table column names matches the
# JSON keys 
staging_events_copy = ("""
    copy staging_events_table
    from 's3://mybucket/category_object_auto.json'
    iam_role 'arn:aws:iam::0123456789012:role/MyRedshiftRole' 
    json 'auto';
    """).format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES
songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
