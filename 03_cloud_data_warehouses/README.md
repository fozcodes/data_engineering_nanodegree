# Sparkify Song Play Analytics DB

This is an ETL and Postgres DB that's intended to make it easy to run analytics
on Sparkify song plays. We take the data from the raw JSON files in the `/data`
directory and loads them into the DB tables in (hopefully) an organized manner.

## Requirements

- Python >= 3.8
- Postgres >= 12
- Make >= 3

## Getting started

### Database

You'll need a local Postgres running. If you don't have one, you can simply run
`docker compose up` in a terminal, and a docker container will be started with
the user:password `postgres:postgres`. If you have your own, you can set
environment variables using whatever environment manager you have:

```
DB_USER
DB_PASSWORD
```

These will be picked up in the `create_tables.py` file for connection

### Virtual environment and dependencies

This is optional, but if you'd like to setup a virtual environment, run:

```
$ make init
$ source ./venv-sparkify/bin/activate
```

Or if you use Fish shell:

```
$ make init
$ source ./venv-sparkify/bin/activate.fish
```

In any case you'll need to install dependencies:

```
$ make install_deps
```

## Running the ETL

To run the ETL:

```
$ make run
```

## Repo files

- `create_tables.py`: Resets the database and creates the tables needed.
- `etl.py`: The main extract, transform, load module. When run it will load all
  the files for songs and songplays. It should be run _after_
  `create_tables.py`.
- `sql_queries.py`: String variables containing necessary SQL queries to run
  the app.

## Design decisions

### Database Schema

The schema is what could be described as a snowflake schema because of the
`valid_plan_levels` table — but it is essentially a star schema. The goal was to
gain as much data integrity as possible while still allowing flexibility for
no-so-great data. The tables can be described as follows:

- `songplays`: The Fact Table. Holds all data related to any songplay by a user.
  These are events. Has foreign keys referencing: `songs`, `artists`,
  `start_times`, `users`, and `valid_plan_levels`.
- `users`: A Dimension Table. Holds all data related to a given user.
- `songs`: A Dimension Table. Holds all data related to a given song.
- `artists`: A Dimension Table. Holds all data related to a given artist.
- `times`: A Dimension Table. Holds all data relatejd to a given datetime entry.
  This table uses Postgres 12+ `GENERATED` feature to generate any metadata from
  a given `start_time`, e.g., `hour`, `week`, `weekday`, etc.
- `valid_plan_levels`: A Dimension Table. Used as an enum, so that we have data
  integrity around plan levels. Any plans we _don't_ know about should raise
  when trying to insert into `users` or `songplays`.

### Create tables / DB reset

For the most part, this wasn't changed - just cleaned up a bit. It was
refactored to use the centralized `db` functions. Also, the drop tables process
was deleted as it was redundant - if you drop the database, you drop the tables.

### ETL Process

The ETL is done from the `etl.py` script. Some logic related to DB config and
connection, parsing files, etc., was broken out into other modules for
organization and to reduce the size of the `etl.py` file.

Functions are made as small as possible with descriptive names. It's best to
follow a Functional Programming style whenever possible to make things easy to
reason about. `pydash` was added for this reason.

## Query examples

With the data in place you can do some interesting analyses. The most insightful
usually come from grouping by song, time, or artist.

### Song plays per week

Query the number of plays per week per song with an ID:

```
SELECT
  COUNT(songplay_id) AS plays,
  s.title as song_plays,
  t.week as week_number
FROM
  songplays AS sp
  INNER JOIN times AS t ON t.start_time = sp.start_time
  INNER JOIN songs AS s ON s.song_id = sp.song_id
WHERE
  sp.song_id IS NOT NULL
GROUP BY
  s.title,
  t.week;
```

The example result:

| plays | song_title                               | week_number |
| ----- | ---------------------------------------- | ----------- |
| 2     | A Higher Place (Album Version)           | 45          |
| 2     | A Higher Place (Album Version)           | 47          |
| 1     | Broken-Down Merry-Go-Round               | 45          |
| 1     | Der Kleine Dompfaff                      | 47          |
| 1     | Face the Ashes                           | 47          |
| 1     | Floating                                 | 45          |
| 1     | Harajuku Girls                           | 46          |
| 1     | Intro                                    | 46          |
| 1     | Intro                                    | 47          |
| 1     | Intro                                    | 48          |
| 1     | Salt In NYC                              | 48          |
| 1     | Setanta matins                           | 47          |
| 3     | Streets On Fire (Explicit Album Version) | 45          |
| 1     | Streets On Fire (Explicit Album Version) | 46          |
| 1     | Streets On Fire (Explicit Album Version) | 48          |
| 1     | The Ballad Of Sleeping Beauty            | 47          |
| 2     | Tonight Will Be Alright                  | 46          |
| 1     | Tonight Will Be Alright                  | 47          |
| 1     | Tonight Will Be Alright                  | 48          |

## TODOs / Improvements

- [ ] Reorganize these files into proper subfolders. Having everything in the
      root isn't great. I couldn't get the modules and configuration to play
      correctly, so I just bailed for now so I could move on to the next
      exercise.
