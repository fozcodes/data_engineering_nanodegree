from typing import List

import psycopg2
from config import get_config, get_opt_parser
from log import config_log
from sql_queries import copy_table_queries, insert_table_queries, run_queries

logging = config_log()
optparser = get_opt_parser()


def main(table_types: List[str]):
    """
    Reads the table_types input and runs the appropriate data loading for each

    :param table_types: List[str]:

    """
    config = get_config()
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    if "staging" in table_types:
        run_queries(cur, conn, copy_table_queries, "Loading staging table with query:")

    if "datamart" in table_types:
        run_queries(
            cur, conn, insert_table_queries, "Loading cleaned table with query:"
        )

    conn.close()


if __name__ == "__main__":
    options, args = optparser.parse_args()
    if options.table_types:
        main(options.table_types.split(","))
    else:
        raise Exception(
            "Missing --table-types option. Available types: staging, datamart"
        )
