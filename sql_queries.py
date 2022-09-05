import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
        (
            event_id INT IDENTITY(0,1),
            artist_name VARCHAR(255),
            auth VARCHAR(50),
            user_first_name VARCHAR(255),
            user_gender  VARCHAR(1),
            item_in_session	INTEGER,
            user_last_name VARCHAR(255),
            song_length	DOUBLE PRECISION, 
            user_level VARCHAR(50),
            location VARCHAR(255),	
            method VARCHAR(25),
            page VARCHAR(35),	
            registration VARCHAR(50),	
            session_id	BIGINT,
            song_title VARCHAR(255),
            status INTEGER,	
            ts VARCHAR(50),
            user_agent TEXT,	
            user_id VARCHAR(100),
            PRIMARY KEY (event_id)
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
        (
            song_id VARCHAR(100),
            num_songs INTEGER,
            artist_id VARCHAR(100),
            artist_latitude DOUBLE PRECISION,
            artist_longitude DOUBLE PRECISION,
            artist_location VARCHAR(255),
            artist_name VARCHAR(255),
            title VARCHAR(255),
            duration DOUBLE PRECISION,
            year INTEGER,
            PRIMARY KEY (song_id)
        );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
        (
        songplay_id SERIAL PRIMARY KEY, 
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
""").format()

staging_songs_copy = ("""
""").format()

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
                         VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                         ON CONFLICT DO NOTHING;
                         """)

user_table_insert = ("""
                    INSERT INTO 
                        users (
                            user_id, 
                            first_name, 
                            last_name, 
                            gender, 
                            level) 
                    VALUES(%s,%s,%s,%s,%s) 
                    ON CONFLICT(user_id) 
                    DO UPDATE SET level = EXCLUDED.level;
                    """)

song_table_insert = ("""
                    INSERT INTO 
                        songs (
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration)
                    VALUES(%s,%s,%s,%s,%s) 
                    ON CONFLICT(song_id) 
                    DO NOTHING;
                    """)

artist_table_insert = ("""
                       INSERT INTO 
                           artists (
                               artist_id, 
                               name, 
                               location, 
                               latitude, 
                               longitude) 
                       VALUES(%s,%s,%s,%s,%s) 
                       ON CONFLICT(artist_id) 
                       DO NOTHING;
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
                     VALUES(%s,%s,%s,%s,%s,%s,%s) 
                     ON CONFLICT DO NOTHING;
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
