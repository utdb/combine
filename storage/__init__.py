import configparser
import storage.postgres


def opendb(configfile):
    """
    Initialize a database connection
    """
    # TODO check if postgres is storage kind
    return storage.postgres.opendb(configfile)
