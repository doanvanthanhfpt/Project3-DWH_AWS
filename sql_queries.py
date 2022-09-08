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
    CREATE TABLE IF NOT EXISTS staging_events
        (
            artist varchar,
            auth varchar,
            firstName varchar,
            gender varchar,
            itemInSession int,
            lastName varchar,
            length float,
            level varchar,
            location varchar,
            method varchar,
            page varchar,
            registration float,
            sessionId int,
            song varchar,
            status int,
            ts timestamp,
            userAgent varchar,
            userId int
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
        (
            num_songs int,
            artist_id varchar,
            artist_latitude varchar,
            artist_longitude varchar,
            artist_location varchar,
            artist_name varchar,
            song_id varchar,
            title varchar,
            duration float,
            year int
        );
""")

# Fact table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
        (
        songplay_id INT PRIMARY KEY, 
        start_time TIMESTAMP NOT NULL, 
        user_id INT NOT NULL, 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id VARCHAR, 
        location VARCHAR, 
        user_agent VARCHAR
        );
""")

# Dimension tables
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users 
        (
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
        );

""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
        (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR, 
        year INT,
        duration FLOAT NOT NULL
        );

""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
        (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL, 
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
        (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday VARCHAR);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from '{}'
    IAM_ROLE '{}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON '{}';
""").format(
        config.get('S3','LOG_DATA'),
        config.get('IAM_ROLE','ARN'),
        config.get('S3','LOG_JSONPATH')
        )

staging_songs_copy = ("""
    COPY staging_songs from '{}'
    IAM_ROLE '{}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON 'auto';
""").format(
        config.get("S3","SONG_DATA"),
        config.get("IAM_ROLE","ARN")
        )

# config.get('AWS','KEY')
# FINAL TABLES

songplay_table_insert = ("""
                         INSERT INTO 
                             songplays (
                                 start_time, 
                                 user_id, 
                                 level, 
                                 song_id, 
                                 artist_id, 
                                 session_id, 
                                 location, 
                                 user_agent) 
                         SELECT st_event.ts AS start_time,
                                 st_event.userId AS user_id,
                                 st_event.level AS level,
                                 st_song.song_id AS song_id,
                                 st_song.artist_id AS artist_id,
                                 st_event.session_id AS session_id,
                                 st_event.location AS location,
                                 st_event.user_agent AS user_agent
                         FROM staging_events AS st_event
                         JOIN staging_songs AS st_song ON (st_event.artist = st_song.artist_name)
                         WHERE st_event.page = 'NextSong';
                         """)

user_table_insert = ("""
                    INSERT INTO 
                        users (
                            user_id, 
                            first_name, 
                            last_name, 
                            gender, 
                            level) 
                    SELECT st_event.user_id AS user_id,
                            st_event.first_name AS first_name,
                            st_event.last_name AS last_name,
                            st_event.gender AS gender,
                            st_event.level AS level
                         FROM staging_events AS st_event
                         WHERE st_event.page = 'NextSong';
                    """)

song_table_insert = ("""
                    INSERT INTO 
                        songs (
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration)
                    SELECT st_song.song_id AS song_id,
                            st_song.title AS title,
                            st_song.artist_id AS artist_id,
                            st_song.year AS year,
                            st_song.duration AS duration
                         FROM staging_songs AS st_song;
                    """)

artist_table_insert = ("""
                       INSERT INTO 
                           artists (
                               artist_id, 
                               name, 
                               location, 
                               latitude, 
                               longitude) 
                    SELECT st_song.artist_id AS artist_id,
                            st_song.name AS name,
                            st_song.location AS location,
                            st_song.latitude AS latitude,
                            st_song.longitude AS longitude
                         FROM staging_songs AS st_song;
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
                    SELECT DISTINCT timestamp 'epoch' + st_event.ts/1000 * interval '1 second' AS start_time,
                            extract(HOUR FROM st_event.timestamp) AS hour,
                            extract(DAY FROM st_event.timestamp) AS day,
                            extract(WEEK FROM st_event.timestamp) AS week,
                            extract(MONTH FROM st_event.timestamp) AS month,
                            extract(YEAR FROM st_event.timestamp) as year,
                            extract(WEEK FROM st_event.timestamp) as weekday
                    FROM staging_events AS st_event
                    WHERE st_event.page = 'NextSong';
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
