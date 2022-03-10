import configparser


def get_config():
    parser = configparser.ConfigParser()
    parser.read("dwh.cfg")
    return parser


def get_opt_parser():
    optparser = optparse.OptionParser()
    optparser.add_option(
        "--table-types",
        "-t",
        dest="table_types",
        help="A comma separated list of the table types you'd like to reset. One or all of: staging | datamart",
    )
    return optparser
