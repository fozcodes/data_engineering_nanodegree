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
    return df.where(pd.notnull(df), None)


def artist_data_from_songfile(df):
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
    song_data = df.filter(items=["song_id", "title", "artist_id", "year", "duration"])
    return clean_not_nulls(song_data)


def process_song_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)

    artist_data = artist_data_from_songfile(df)
    cur.execute(artist_table_insert, artist_data.values[0])

    song_data = song_data_from_songfile(df)
    cur.execute(song_table_insert, song_data.values[0])


def only_next_song_data(df):
    # use .copy to avoid the SettingWithCopyWarning
    return df.loc[df["page"] == "NextSong"].copy()


def datetime_from_mills_column(df, column_title):
    return pd.to_datetime(df[column_title], unit="ms", utc=True)


def user_data_from_songplays(df):
    return df.filter(items=["userId", "firstName", "lastName", "gender", "level"])


def song_query_variables_not_null(row):
    return (
        not py_([row.song, row.artist, row.length])
        .every(lambda v: v is not None)
        .value()
    )


def load_start_times(next_song_data, cursor):
    # insert time data records
    start_times = sorted_uniq(list(next_song_data["startTime"]))

    for t in start_times:
        cursor.execute(time_table_insert, [t])


def load_users(next_song_data, cursor):
    # load user table
    user_data = user_data_from_songplays(next_song_data)

    # insert user records
    for _, row in user_data.iterrows():
        cursor.execute(user_table_insert, row)


def load_songplays(next_song_data, cursor):
    # insert songplay records
    for _, row in next_song_data.iterrows():

        if song_query_variables_not_null(row):
            print(f"Empty values for song, artist, or length in log {row}")
            print([row.song, row.artist, row.length])
            continue

        # get songid and artistid from song and artist tables
        #         print(row.song)
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
    songplay_dataframe = pd.read_json(filepath, lines=True)

    next_song_data = only_next_song_data(songplay_dataframe)

    next_song_data["startTime"] = datetime_from_mills_column(next_song_data, "ts")

    load_start_times(next_song_data, cursor)
    load_users(next_song_data, cursor)
    load_songplays(next_song_data, cursor)


def get_all_json_files_in_path(filepath):
    # refactor using pydash , no pyramids
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = get_all_json_files_in_path(filepath)
    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    conn = psycopg2.connect(
        f"host={DB_HOST} dbname=sparkifydb user={DB_USER} password={DB_PASSWORD}"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
