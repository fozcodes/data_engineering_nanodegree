import optparse
from typing import List

import psycopg2
from config import get_config
from log import config_log
from sql_queries import (create_datamart_table_queries,
                         create_staging_table_queries,
                         drop_datamart_table_queries,
                         drop_staging_table_queries)

logging = config_log()

optparser = optparse.OptionParser()
optparser.add_option(
    "--table-types",
    "-t",
    dest="table_types",
    help="A comma separated list of the table types you'd like to reset. One or all of: staging | datamart",
)

TABLE_TYPES = {
    "staging": {
        "drop": drop_staging_table_queries,
        "create": create_staging_table_queries,
    },
    "datamart": {
        "drop": drop_datamart_table_queries,
        "create": create_datamart_table_queries,
    },
}


def drop_tables(cur, conn, queries):
    for query in queries:
        logging.info(f"Dropping table with query: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, queries):
    for query in queries:
        logging.info(f"Creating table with query: {query}")
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
        drop_tables(cur, conn, drop_staging_table_queries)
        create_tables(cur, conn, create_staging_table_queries)

    if "datamart" in table_types:
        drop_tables(cur, conn, drop_datamart_table_queries)
        create_tables(cur, conn, create_datamart_table_queries)

    conn.close()


if __name__ == "__main__":
    options, args = optparser.parse_args()
    if options.table_types:
        main(options.table_types.split(","))
    else:
        raise Exception(
            "Missing --table-types option. Available types: staging, datamart"
        )
