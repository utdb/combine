import sys
import configparser
import psycopg2

class postgresdb:

    def __init__(self,conn):
        self.conn = conn
        print("INIT DB")

    def create(self):
        try:
            cur = self.conn.cursor()
            stat = """
                  CREATE TABLE objects (
		      id bigserial PRIMARY KEY,
		      value	TEXT
                  );
                  CREATE TABLE provenance (
		      src bigint,
		      dst bigint
                  );
               """
            cur.execute(stat)
            self.conn.commit()
        except Exception as ex:
            handle_db_error("create",ex)

    def destroy(self):
        # TODO during development just empty the database
        # drop schema public cascade;
        # create schema public;
        try:
            cur = self.conn.cursor()
            stat = """
                  DROP TABLE IF EXISTS objects    CASCADE;
                  DROP TABLE IF EXISTS provenance CASCADE;
               """
            cur.execute(stat)
            self.conn.commit()
        except Exception as ex:
            handle_db_error("destroy",ex)

    def add_object(self,v):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO objects (value) VALUES (%s);",[v])
            stat = "SELECT currval(pg_get_serial_sequence(\'objects\',\'id\'));"
            cur.execute(stat)
            rows = cur.fetchall()
            self.conn.commit()
            return rows[0][0]
        except Exception as ex:
            handle_db_error("add_object",ex)

    def add_provenance(self,src,dst):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO provenance (src,dst) VALUES (%s,%s);",[src,dst])
            self.conn.commit()
        except Exception as ex:
            handle_db_error("add_provenance",ex)

def handle_db_error(what, ex):
    print(what+": "+str(ex))
    # sys.exit()

def opendb(configfile):
    """
    Initialize a postgres connection
    """
    config = configparser.RawConfigParser()
    config.read(configfile)
    try:
        conn = psycopg2.connect("dbname='"+config.get("postgresdb", "db")+"' user='"+config.get("postgresdb", "user")+"' host='"+config.get("postgresdb", "host")+"' password='"+config.get("postgresdb", "password")+"'")
    except Exception as ex:
        handle_db_error("Unable to connect to postgres"+str(ex))

    # conn.cursor().execute("CREATE TABLE tab (dummy int);")
    pdb = postgresdb(conn)
    return pdb
