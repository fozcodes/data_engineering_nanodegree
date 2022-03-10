from typing import List

import psycopg2
from config import get_config, get_opt_parser
from sql_queries import (create_datamart_table_queries,
                         create_staging_table_queries,
                         drop_datamart_table_queries,
                         drop_staging_table_queries, run_queries)

optparser = get_opt_parser()


def main(table_types: List[str]):
    """
    Runs the correct table drop/create for any table type found in table_types

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
        run_queries(cur, conn, drop_staging_table_queries, "Dropping table with query:")
        run_queries(
            cur, conn, create_staging_table_queries, "Creating table with query:"
        )

    if "datamart" in table_types:
        run_queries(
            cur, conn, drop_datamart_table_queries, "Dropping table with query:"
        )
        run_queries(
            cur, conn, create_datamart_table_queries, "Creating table with query:"
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
