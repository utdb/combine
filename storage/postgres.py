import sys
import configparser
import psycopg2

class postgres:

    def __init__(self,conn):
        self.conn = conn
        print("INIT DB")

    def create(self):
        try:
            cur = self.conn.cursor()
            stat = """
                  CREATE SEQUENCE combine_global_id;

                  CREATE TABLE job (
		      jid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      name              TEXT,
                      description       TEXT,
                      createtime        TIMESTAMP,
                      starttime         TIMESTAMP,
                      stoptime          TIMESTAMP
                  );

                  CREATE TABLE object (
		      oid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      time         TIMESTAMP,
                      avid         BIGINT,
                      kind         TEXT,
                      content_type TEXT,
                      content      BYTEA
                  );
                  CREATE TABLE activity (
		      aid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      createtime TIMESTAMP,
                      jid        BIGINT,
                      module     TEXT
                  );
                  CREATE TABLE activity_trigger (
                      aid       BIGINT,
                      kind      TEXT,
                      tag       TEXT
                  );
                  CREATE TABLE activation (
		      avid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      createtime  TIMESTAMP,
                      aid         BIGINT
                  );
                  CREATE TABLE activation_in (
		      avid BIGINT,
                      oid  BIGINT
                  );
                  CREATE TABLE activation_out (
		      avid  BIGINT,
                      oid   BIGINT
                  );
                  CREATE TABLE log (
		      lid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      time      TIMESTAMP,
                      xid        BIGINT,
                      event     TEXT,
                      data      TEXT
                  );
                  CREATE VIEW active_job AS
                      SELECT * from job 
                      WHERE (stoptime IS NULL) AND NOT (starttime IS NULL);
                  CREATE VIEW active_activity AS
                      SELECT activity.aid, activity.module FROM activity,active_job
                      WHERE active_job.jid = activity.jid;
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
                  DROP TABLE IF EXISTS job    CASCADE;
                  DROP TABLE IF EXISTS object     CASCADE;
                  DROP TABLE IF EXISTS actions    CASCADE;
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



    def add_job(self,name,description):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO job (name,description,createtime) VALUES (%s,%s,clock_timestamp());",[name,description])
            cur.execute("select last_value from combine_global_id;")
            jid = singlevalue(cur)
            self.conn.commit()
            return jid
        except Exception as ex:
            handle_db_error("add_job",ex)

    def add_activity(self,jid,module,triggerseq):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO activity (createtime,jid,module) VALUES (clock_timestamp(),%s,%s);",[jid,module])
            cur.execute("select last_value from combine_global_id;")
            aid = singlevalue(cur)
            for trigger in triggerseq:
                cur.execute("INSERT INTO activity_trigger (aid,kind,tag) VALUES (%s,%s,%s);",[aid,trigger[0],trigger[1]])
            self.conn.commit()
            return aid
        except Exception as ex:
            handle_db_error("add_activity",ex)

    def add_activation(self,aid):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO activation(createtime,aid) VALUES (clock_timestamp(),%s);",[aid])
            cur.execute("select last_value from combine_global_id;")
            avid = singlevalue(cur)
            self.conn.commit()
            return avid
        except Exception as ex:
            handle_db_error("add_activation",ex)

    def add_object(self,avid,kind,content_type,content):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO object (time,avid,kind,content_type,content) VALUES (clock_timestamp(),%s,%s,%s,%s);",[avid,kind,content_type,content])
            cur.execute("select last_value from combine_global_id;")
            oid = singlevalue(cur)
            self.conn.commit()
            return oid
        except Exception as ex:
            handle_db_error("add_object",ex)


    def set_activation_graph(self,avid,inseq,outseq):
        try:
            cur = self.conn.cursor()
            for n in inseq:
                cur.execute("INSERT INTO activation_in  (avid,oid) VALUES (%s,%s);",[avid,n])
            for n in outseq:
                cur.execute("INSERT INTO activation_out  (avid,oid) VALUES (%s,%s);",[avid,n])
            self.conn.commit()
        except Exception as ex:
            handle_db_error("add_object",ex)

    def add_log(self,xid,event,data):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO log (time,xid,event,data) VALUES (clock_timestamp(),%s,%s,%s);",[xid,event,data])
            cur.execute("select last_value from combine_global_id;")
            lid = singlevalue(cur)
            return lid
        except Exception as ex:
            handle_db_error("add_log",ex)

    def get_object(self,oid):
        return PgObject(self,oid)

    def get_job(self,jid):
        return PgJob(self,jid)

def singlevalue(cur):
    # TODO check if it is really a single value
    if cur.rowcount == 1:
        rows = cur.fetchall()
        return rows[0][0]
    else:
        raise Exception('postgres result not a single value')

class PgWrapper:
   def __init__(self,db,table,idattr,idvalue):
       self.db = db
       self.table = table
       self.idattr = idattr
       self.idvalue = idvalue

   def __getattr__(self, name):

       def _try_retrieve(*args, **kwargs):
           try:
               cur = self.db.conn.cursor()
               cur.execute("SELECT "+name+" FROM "+self.table+" WHERE "+self.idattr+"="+str(self.idvalue)+";")
               return singlevalue(cur)
           except Exception as ex:
               handle_db_error("_try_retrieve",ex)

       return _try_retrieve

class PgObject(PgWrapper):

   def __init__(self,db,idvalue):
       super(PgObject, self).__init__(db,"object","oid",idvalue)

class PgJob(PgWrapper):

   def __init__(self,db,idvalue):
       super(PgJob, self).__init__(db,"job","jid",idvalue)

   def start(self):
       try:
           cur = self.db.conn.cursor()
           cur.execute("UPDATE "+self.table+" SET starttime=clock_timestamp(), stoptime=NULL WHERE "+self.idattr+"="+str(self.idvalue)+";")
           self.db.conn.commit()
       except Exception as ex:
           handle_db_error("_try_retrieve",ex)
        

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
        conn = psycopg2.connect("dbname='"+config.get("postgres", "db")+"' user='"+config.get("postgres", "user")+"' host='"+config.get("postgres", "host")+"' password='"+config.get("postgres", "password")+"'")
    except Exception as ex:
        handle_db_error("Unable to connect to postgres"+str(ex))

    # conn.cursor().execute("CREATE TABLE tab (dummy int);")
    pdb = postgres(conn)
    return pdb

