# Sparkify Song Play Analytics DB

This is an ETL and Postgres DB that's intended to make it easy to run analytics
on Sparkify song plays. We take the data from the raw JSON files in the `/data`
directory and loads them into the DB tables in (hopefully) an organized manner.

## Requirements

- Python >= 3.8
- Postgres >= 12

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
$ python -m venv venv-sparkify
$ source ./venv-sparkify/bin/activate.{WHICHEVER SHELL YOU USE}
```

In either case you'll need to install dependencies:

```
pip install -r requirements.txt
```

## Repo files

## Design decisions

## Query examples
