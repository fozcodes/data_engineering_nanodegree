# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"
valid_plan_level_drop = "DROP TABLE IF EXISTS valid_plan_levels"

# CREATE TABLES

valid_plan_level_create = """
CREATE TABLE IF NOT EXISTS valid_plan_levels (
    level VARCHAR(255) PRIMARY KEY
);

INSERT INTO valid_plan_levels (level) VALUES
('free'),
('paid');
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL,
    start_time timestamp without time zone REFERENCES times (start_time) ON UPDATE CASCADE,
    user_id integer REFERENCES users (user_id) ON UPDATE CASCADE,
    level VARCHAR(255) REFERENCES valid_plan_levels (level) ON UPDATE CASCADE,
    song_id VARCHAR(255) REFERENCES songs (song_id) ON UPDATE CASCADE,
    artist_id VARCHAR(255) REFERENCES artists (artist_id) ON UPDATE CASCADE,
    session_id integer NOT NULL,
    location TEXT NOT NULL,
    user_agent TEXT NOT NULL
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id integer PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    gender VARCHAR(255) NOT NULL,
    level VARCHAR(255) REFERENCES valid_plan_levels (level) ON UPDATE CASCADE
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    artist_id VARCHAR(255) REFERENCES artists (artist_id) ON UPDATE CASCADE,
    year integer NOT NULL ,
    duration decimal(5) NOT NULL
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    latitude decimal(4),
    longitude decimal(4)
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS times (
    start_time timestamp without time zone PRIMARY KEY,
    hour integer GENERATED ALWAYS AS (DATE_PART('hour', start_time)) STORED,
    day integer GENERATED ALWAYS AS (DATE_PART('day', start_time)) STORED,
    week integer GENERATED ALWAYS AS (DATE_PART('week', start_time)) STORED,
    month integer GENERATED ALWAYS AS (DATE_PART('month', start_time)) STORED,
    year integer GENERATED ALWAYS AS (DATE_PART('year', start_time)) STORED,
    weekday integer GENERATED ALWAYS AS (DATE_PART('dow', start_time)) STORED
)

"""

# INSERT RECORDS

songplay_table_insert = """
    INSERT INTO songplays (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT
    DO NOTHING;

"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
"""

song_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s);
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING;
"""


time_table_insert = "INSERT INTO times (start_time) VALUES (%s)"

# FIND SONGS BY song title, artist name, and song duration

song_select = """
    SELECT
        s.song_id,
        a.artist_id
    FROM
        songs as s
    INNER JOIN
        artists as a ON a.artist_id = s.artist_id
    WHERE
        s.title = %s
        OR a.name = %s
"""

# QUERY LISTS

create_table_queries = [
    valid_plan_level_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create,
]
