import os

import psycopg2

DB_HOST = "localhost"
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "sparkifydb")


def create_connection():
    """
    Creates a connection to the postgres server without selecting a database.
    """
    return psycopg2.connect(f"host={DB_HOST} user={DB_USER} password={DB_PASSWORD}")


def create_db_connection():
    """
    Creates a connection to the postgres server selecting the application database.
    """
    return psycopg2.connect(
        f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
    )
