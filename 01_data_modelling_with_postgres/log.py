import logging


def config_log(level=logging.INFO):
    """Centralized place to configure logs"""
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    return logging
