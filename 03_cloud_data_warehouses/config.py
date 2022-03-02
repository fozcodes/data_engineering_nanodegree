import configparser


def get_config():
    parser = configparser.ConfigParser()
    parser.read("dwh.cfg")
    return parser
