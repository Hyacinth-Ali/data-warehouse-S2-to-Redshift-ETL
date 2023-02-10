import configparser

# CONFIGURATION
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Load datawarehouse parameter values
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME = config.get("DWH","DWH_IAM_ROLE_NAME")

ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLE QUERIES

# Reset staging song events table
staging_events_table_drop = "DROP TABLE staging_events_table"

# Reset staging song data table
staging_songs_table_drop = "DROP TABLE staging_songs_table"

# Reset song play data table
songplay_table_drop = "DROP TABLE songplay_table"

# Reset users table to facilitate testing the ETL pipeline
user_table_drop = "DROP TABLE user_table"

# Reset songs table to facilitate testing the ETL pipeline
song_table_drop = "DROP TABLE song_table"

# Reset songs table to facilitate testing the ETL pipeline
artist_table_drop = "DROP TABLE artist_table"

# Reset songs table to facilitate testing the ETL pipeline
time_table_drop = "DROP TABLE time_table CASCADE"

# CREATE STAGING TABLES
# Create staging song events table
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events_table 
        (
            artist text, 
            auth text,
            firstName text,
            gender text,
            itemInSession int4,
            lastName text,
            length float,
            level text,
            location text,
            method text,
            page text,
            registration float,
            sessionId int4,
            song text,
            status int4,
            ts bigint,
            userAgent text,
            userId int
        )
""")

# Create staging song dataset
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs_table
    (
      num_songs int4,
      artist_id text,
      artist_latitude float,
      artist_longitude float,
      artist_location text,
      artist_name text,
      song_id text,
      title text,
      duration float,
      year int

    )
""")

# CREATE FACT - song play

# Create song play query
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table
    (
        songplay_id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        start_time date REFERENCES time_table,
        user_id INT4 REFERENCES user_table,
        level TEXT,
        song_id TEXT REFERENCES song_table,
        artist_id TEXT REFERENCES artist_table,
        sessionId int4,
        location TEXT,
        user_agent TEXT
    );
""")

# CREATE DIMENSIONAL TABLES - user, song, artist, and time tables

# Create User Table
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table
    (
        user_id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        level TEXT
    );
""")

# Create song table
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_table
    (
        song_id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        title TEXT,
        artist_id TEXT,
        year INT4,
        duration float
    );
""")

# Create artist table
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_table
    (
        artist_id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        name TEXT,
        location TEXT,
        latitude float,
        longitude float
    );
""")

# Create time table
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table
    (
        time_id INT4 IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        start_time TIMESTAMP,
        hour INT4,
        day INT4,
        week INT4,
        month INT4,
        year INT4,
        weekday TEXT
    );
""")

# STAGING TABLES - Load data from S3 and stage them in Redshift databases
# Using auto option since the event table column names matches the
# JSON keys 
#copy staging event datasets
staging_events_copy = ("""
    COPY staging_events_table FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    json 's3://udacity-dend/log_json_path.json'
    dateformat 'auto';
""").format(ARN)

#copy staging song data datasets
staging_songs_copy = ("""
    COPY staging_songs_table FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    json 'auto';
""").format(ARN)


# FINAL TABLES

# insert data into song play table
songplay_table_insert = ("""
    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, sessionId, location, user_agent)
    SELECT
        to_timestamp(ts, 'YYYYMMDD HHMISS') AS start_time,
        userId AS user_id,
        level,
        song_id,
        artist_id,
        sessionId,
        location,
        userAgent AS user_agent
    FROM staging_events_table e
    JOIN staging_songs_table s
    ON (e.artist = s.artist_name)
    AND (e.length = s.duration)
    AND (e.song = s.title)
    AND page = 'NextSong'
    ;
""")

# insert data into user table
user_table_insert = ("""
    INSERT INTO user_table (first_name, last_name, gender, level)
    SELECT DISTINCT
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events_table;
""")

# insert data into song table
song_table_insert = ("""
    INSERT INTO song_table (title, artist_id, year, duration)
    SELECT DISTINCT
        title,
        artist_id,
        year,
        duration
    FROM staging_songs_table;
""")

# Insert data into artist table
artist_table_insert = ("""
    INSERT INTO artist_table (name, location, latitude, longitude)
    SELECT DISTINCT
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs_table;
""")

# insert data into time table
time_table_insert = ("""
    INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(weekday FROM start_time) AS weekday
    FROM songplay_table;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
