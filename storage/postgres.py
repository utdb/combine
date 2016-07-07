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
                  CREATE SEQUENCE combine_global_id;

                  CREATE TABLE objects (
		      id BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      time         TIMESTAMP,
                      activity_id  BIGINT,
                      kind         TEXT,
                      content_type TEXT,
                      content      TEXT

                  );
                  CREATE TABLE activity (
		      id BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      createtime TIMESTAMP,
                      job        BIGINT,
                      module     TEXT
                  );
                  CREATE TABLE activity_trigger (
                      activity_id       BIGINT,
                      kind              TEXT,
                      tag               TEXT
                  );
                  CREATE TABLE activation (
		      id BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      createtime        TIMESTAMP,
                      activity_id       BIGINT
                  );
                  CREATE TABLE activation_in (
		      activation_id BIGINT,
                      in_id         BIGINT
                  );
                  CREATE TABLE activation_out (
		      activation_id  BIGINT,
                      out_id         BIGINT
                  );
                  CREATE TABLE log (
                      time      TIMESTAMP,
                      id        BIGINT,
                      event     TEXT,
                      data      TEXT
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
                  DROP TABLE IF EXISTS actions    CASCADE;
                  DROP TABLE IF EXISTS objects    CASCADE;
                  DROP TABLE IF EXISTS provenance CASCADE;
                  DROP TABLE IF EXISTS activity    CASCADE;
                  DROP TABLE IF EXISTS activity_trigger    CASCADE;
                  DROP TABLE IF EXISTS activation    CASCADE;
                  DROP TABLE IF EXISTS activation_in    CASCADE;
                  DROP TABLE IF EXISTS activation_out    CASCADE;
                  DROP TABLE IF EXISTS log        CASCADE;
                  DROP SEQUENCE IF EXISTS combine_global_id CASCADE;
               """
            cur.execute(stat)
            self.conn.commit()
        except Exception as ex:
            handle_db_error("destroy",ex)

    def add_activity(self,job,module):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO activity (createtime,job,module) VALUES (clock_timestamp(),%s,%s);",[job,module])
            stat = "SELECT currval(pg_get_serial_sequence(\'activity\',\'id\'));"
            cur.execute(stat)
            rows = cur.fetchall()
            self.conn.commit()
            return rows[0][0]
        except Exception as ex:
            handle_db_error("add_activity",ex)

    def add_activation(self,activity_id):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO activation(createtime,activity_id) VALUES (clock_timestamp(),%s);",[activity_id])
            stat = "SELECT currval(pg_get_serial_sequence(\'activation\',\'id\'));"
            cur.execute(stat)
            rows = cur.fetchall()
            self.conn.commit()
            return rows[0][0]
        except Exception as ex:
            handle_db_error("add_activation",ex)

    def add_object(self,activity_id,kind,content_type,content):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO objects (time,activity_id,kind,content_type,content) VALUES (clock_timestamp(),%s,%s,%s,%s);",[activity_id,kind,content_type,content])
            stat = "SELECT currval(pg_get_serial_sequence(\'objects\',\'id\'));"
            cur.execute(stat)
            rows = cur.fetchall()
            id = rows[0][0]
            self.conn.commit()
            return id
        except Exception as ex:
            handle_db_error("add_object",ex)


    def set_activation_graph(self,activation_id,inseq,outseq):
        try:
            cur = self.conn.cursor()
            for n in inseq:
                cur.execute("INSERT INTO activation_in  (activation_id,in_id) VALUES (%s,%s);",[activation_id,n])
            for n in outseq:
                cur.execute("INSERT INTO activation_out  (activation_id,out_id) VALUES (%s,%s);",[activation_id,n])
            self.conn.commit()
            return id
        except Exception as ex:
            handle_db_error("add_object",ex)

    def add_log(self,id,event,data):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO log (time,id,event,data) VALUES (clock_timestamp(),%s,%s,%s);",[id,event,data])
        except Exception as ex:
            handle_db_error("add_log",ex)

def handle_db_error(what, ex):
    print(what+": "+str(ex))
    sys.exit()

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
