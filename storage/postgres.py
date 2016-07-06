import configparser
import psycopg2

class postgresdb:

    def __init__(self,conn):
        self.conn = conn

    def init(self):
        print("INIT DB")

    def create(self):
        cur = self.conn.cursor()
        stat = "CREATE TABLE tab (dummy int);"
        try:
            cur.execute(stat)
        except Exception as ex:
            print("Postgres DB error on: "+stat+" ::: "+str(ex))
        self.conn.commit()

    def destroy(self):
        cur = self.conn.cursor()
        stat = "DROP TABLE tab;"
        try:
            cur.execute(stat)
        except Exception as ex:
            print("Postgres DB error on: "+stat+" ::: "+str(ex))
        self.conn.commit()


def opendb(configfile):
    """
    Initialize a postgres connection
    """
    config = configparser.RawConfigParser()
    config.read(configfile)
    try:
        conn = psycopg2.connect("dbname='"+config.get("postgresdb", "db")+"' user='"+config.get("postgresdb", "user")+"' host='"+config.get("postgresdb", "host")+"' password='"+config.get("postgresdb", "password")+"'")
    except:
        print("I am unable to connect to the database.")

    # conn.cursor().execute("CREATE TABLE tab (dummy int);")
    pdb = postgresdb(conn)
    return pdb
