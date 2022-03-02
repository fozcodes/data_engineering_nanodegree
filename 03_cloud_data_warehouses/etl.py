import optparse
from typing import List

import psycopg2
from config import get_config
from log import config_log
from sql_queries import copy_table_queries, insert_table_queries

logging = config_log()

optparser = optparse.OptionParser()
optparser.add_option(
    "--table-types",
    "-t",
    dest="table_types",
    help="A comma separated list of the table types you'd like to reset. One or all of: staging | datamart",
)


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        logging.info(f"Loading staging table with query: {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        logging.info(f"Loading cleaned table with query: {query}")
        cur.execute(query)
        conn.commit()


def main(table_types: List[str]):
    config = get_config()
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    if "staging" in table_types:
        load_staging_tables(cur, conn)

    if "datamart" in table_types:
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    options, args = optparser.parse_args()
    if options.table_types:
        main(options.table_types.split(","))
    else:
        raise Exception(
            "Missing --table-types option. Available types: staging, datamart"
        )
