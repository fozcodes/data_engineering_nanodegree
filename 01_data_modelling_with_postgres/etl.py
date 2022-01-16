import glob
import os

import pandas as pd
import psycopg2
from pydash import py_
from pydash.arrays import sorted_uniq

from db_config import DB_HOST, DB_PASSWORD, DB_USER
from sql_queries import (artist_table_insert, song_select, song_table_insert,
                         songplay_table_insert, time_table_insert,
                         user_table_insert)


def clean_not_nulls(df):
    """Converts notnull values to None.

    This is usually good for making sure values that _should_ be NULL in a db
    are NULL.

    :param df: pd.DataFrame
    """
    return df.where(pd.notnull(df), None)


def artist_data_from_songfile(df):
    """Extracts artist data from songfile dataframe.

    Also ensures that NaN fields and similar are set to None.

    :param df: pd.DataFrame
    """
    artist_data = df.filter(
        items=[
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    )
    return clean_not_nulls(artist_data)


def song_data_from_songfile(df):
    """Extracts song data from a songfile dataframe.

    Also ensures that NaN fields and similar are set to None.

    :param df: pd.DataFrame
    """
    song_data = df.filter(items=["song_id", "title", "artist_id", "year", "duration"])
    return clean_not_nulls(song_data)


def process_song_file(cur, filepath):
    """Takes data from a songfile at a given filepath and inserts the relevant
    data to the database.

    :param cur: psycopg2 cursor
    :param filepath: str

    """
    df = pd.read_json(filepath, lines=True)

    artist_data = artist_data_from_songfile(df)
    cur.execute(artist_table_insert, artist_data.values[0])

    song_data = song_data_from_songfile(df)
    cur.execute(song_table_insert, song_data.values[0])


def only_next_song_data(df):
    """Filters out data from songplay dataframe that is not a "NextSong" page.

    :param df: pd.DataFrame

    """
    # use .copy to avoid the SettingWithCopyWarning
    return df.loc[df["page"] == "NextSong"].copy()


def datetime_from_mills_column(df: pd.DataFrame, timestamp_column: str):
    """Creates a DateTime column from a timestamp column in a dataframe.

    :param df: pd.DataFrame
    :param timestamp_column: str

    """
    return pd.to_datetime(df[timestamp_column], unit="ms", utc=True)


def user_data_from_songplays(df):
    """Extracts user data from a songplays dataframe.

    :param df: pd.DataFrame

    """
    return df.filter(items=["userId", "firstName", "lastName", "gender", "level"])


def song_query_variables_not_null(row):
    """Checks to see if the properties song, artist, and length are on a dict.

    :param row: dict

    """
    return (
        not py_([row.song, row.artist, row.length])
        .every(lambda v: v is not None)
        .value()
    )


def load_start_times(next_song_data, cursor):
    """Loads start_times data into the DB from songplay data.

    :param next_song_data: pd.DataFrame
    :param cursor: psycopg2 cursor

    """
    start_times = sorted_uniq(list(next_song_data["startTime"]))

    for t in start_times:
        cursor.execute(time_table_insert, [t])


def load_users(next_song_data, cursor):
    """Loads users data into the DB from songplay data.

    :param next_song_data: pd.DataFrame
    :param cursor: psycopg2 cursor

    """
    user_data = user_data_from_songplays(next_song_data)

    for _, row in user_data.iterrows():
        cursor.execute(user_table_insert, row)


def load_songplays(next_song_data, cursor):
    """Loads songplays data into the DB from a dataframe.

    :param next_song_data: pd.DataFrame with only "NextSong" data
    :param cursor: psycopg2 cursor

    """
    for _, row in next_song_data.iterrows():

        if song_query_variables_not_null(row):
            print(f"Empty values for song, artist, or length in log {row}")
            print([row.song, row.artist, row.length])
            continue

        # get songid and artistid from song and artist tables
        cursor.execute(song_select, (row.song, row.artist))
        results = cursor.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = [
            row.startTime,
            row.userId,
            row.level,
            song_id,
            artist_id,
            row.sessionId,
            row.location,
            row.userAgent,
        ]
        cursor.execute(songplay_table_insert, songplay_data)


def process_log_file(cursor, filepath):
    """Process a log file from a filepath.

    :param cursor: psycopg2 cursor
    :param filepath: str - path to a log file

    """
    songplay_dataframe = pd.read_json(filepath, lines=True)

    next_song_data = only_next_song_data(songplay_dataframe)

    next_song_data["startTime"] = datetime_from_mills_column(next_song_data, "ts")

    load_start_times(next_song_data, cursor)
    load_users(next_song_data, cursor)
    load_songplays(next_song_data, cursor)


def get_all_json_files_in_path(filepath):
    """Get all JSON files in a a given filepath

    :param filepath: str - path to walk for possible files

    """
    # refactor using pydash , no pyramids
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def process_data(cur, conn, filepath, func):
    """Main data processing function.

    This function takes in a root filepath where all data is to be processed and
    a file processing function. It iterates through all found files and calls
    the file processing function with the filepath.

    :param cur:
    :param conn:
    :param filepath:
    :param func:

    """
    all_files = get_all_json_files_in_path(filepath)
    number_of_files = len(all_files)
    print(f"{number_of_files} files found in {filepath}")

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{number_of_files} files processed.")


def main():
    """Main entrypoint function."""

    conn = psycopg2.connect(
        f"host={DB_HOST} dbname=sparkifydb user={DB_USER} password={DB_PASSWORD}"
    )
    cur = conn.cursor()

    process_data(cur, conn, "data/song_data", process_song_file)
    process_data(cur, conn, "data/log_data", process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
