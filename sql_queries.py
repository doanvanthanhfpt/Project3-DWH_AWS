import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# Staging tables
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS 
        staging_events (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender VARCHAR,
            itemInSession INT,
            lastName VARCHAR,
            length FLOAT,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration FLOAT,
            sessionId INT,
            song VARCHAR,
            status INT,
            ts BIGINT,
            userAgent VARCHAR,
            userId INT
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
        staging_songs (
            num_songs INT,
            artist_id VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id TEXT,
            title VARCHAR,
            duration FLOAT,
            year INT
        );
""")

# Fact table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
        songplay (
            songplay_id TEXT, 
            start_time TIMESTAMP NOT NULL, 
            user_id BIGINT NOT NULL, 
            level VARCHAR, 
            song_id TEXT, 
            artist_id VARCHAR, 
            session_id VARCHAR, 
            location VARCHAR, 
            user_agent VARCHAR
        );
""")

# Dimension tables
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
        users (
            user_id BIGINT PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            level VARCHAR
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
        songs (
            song_id TEXT PRIMARY KEY,
            title VARCHAR NOT NULL,
            artist_id VARCHAR, 
            year INT,
            duration FLOAT NOT NULL
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
        artists (
            artist_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL, 
            location VARCHAR,
            latitude FLOAT,
            longitude FLOAT
        );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
        time (
            start_time TIMESTAMP,
            hour INT, 
            day INT, 
            week INT, 
            month INT, 
            year INT, 
            weekday VARCHAR
        );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {}
    IAM_ROLE '{}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON {};
""").format(
        config.get('S3','LOG_DATA'),
        config.get('IAM_ROLE','ARN'),
        config.get('S3','LOG_JSONPATH')
        )

staging_songs_copy = ("""
    COPY staging_songs from {}
    IAM_ROLE '{}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON 'auto';
""").format(
        config.get("S3","SONG_DATA"),
        config.get("IAM_ROLE","ARN")
        )


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO 
        songplay (
            start_time, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent) 
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (st_event.ts / 1000) * INTERVAL '1 second' as start_time,
        st_event.userId AS user_id,
        st_event.level AS level,
        st_song.song_id AS song_id,
        st_song.artist_id AS artist_id,
        st_event.sessionId AS session_id,
        st_event.location AS location,
        st_event.userAgent AS user_agent
    FROM 
        staging_events AS st_event
    INNER JOIN 
        staging_songs AS st_song 
    ON st_event.song = st_song.title
    AND st_event.artist = st_song.artist_name
    AND st_event.length = st_song.duration
    AND st_event.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO 
        users (
            user_id, 
            first_name, 
            last_name, 
            gender, 
            level) 
    SELECT 
        st_event.userId AS user_id,
        st_event.firstName AS first_name,
        st_event.lastName AS last_name,
        st_event.gender AS gender,
        st_event.level AS level
    FROM 
        staging_events AS st_event
    WHERE 
        st_event.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO 
        songs (
            song_id, 
            title, 
            artist_id, 
            year, 
            duration)
    SELECT 
        st_song.song_id AS song_id,
        st_song.title AS title,
        st_song.artist_id AS artist_id,
        st_song.year AS year,
        st_song.duration AS duration
    FROM 
        staging_songs AS st_song;
""")

artist_table_insert = ("""
    INSERT INTO 
        artists (
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude) 
    SELECT 
        st_song.artist_id AS artist_id,
        st_song.title AS name,
        st_song.artist_location AS location,
        st_song.artist_longitude AS latitude,
        st_song.artist_longitude AS longitude
    FROM 
        staging_songs AS st_song;
""")

time_table_insert = ("""
    INSERT INTO 
        time (
            start_time, 
            hour, 
            day, 
            week, 
            month, 
            year, 
            weekday) 
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as start_time,
        extract(HOUR FROM start_time) AS hour,
        extract(DAY FROM start_time) AS day,
        extract(WEEK FROM start_time) AS week,
        extract(MONTH FROM start_time) AS month,
        extract(YEAR FROM start_time) as year,
        extract(WEEK FROM start_time) as weekday
        FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
