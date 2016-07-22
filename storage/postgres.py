import sys, traceback
import logging
import configparser
import psycopg2
import psycopg2.extras

class postgres:

    def __init__(self,conn):
        self.conn = conn
        logging.info("Open Postgres DB: "+str(self.conn))

    def closedb(self):
        self.conn.close()
        logging.info("Close Postgres DB: "+str(self.conn))

    def create(self):
        try:
            cur = self.conn.cursor()
            stat = """
                  CREATE SEQUENCE combine_global_id;

                  CREATE TABLE context (
		      cid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      name              TEXT,
                      description       TEXT
                  );

                  CREATE TABLE job (
                      jid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      cid               BIGINT,
                      name              TEXT,
                      description       TEXT,
                      createtime        TIMESTAMP,
                      starttime         TIMESTAMP,
                      stoptime          TIMESTAMP
                  );

                  CREATE TABLE object (
                      oid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      jid          BIGINT,
                      time         TIMESTAMP,
                      avid         BIGINT,
                      kind         TEXT,
                      tags         TEXT[],
                      content_type TEXT,
                      content      BYTEA
                  );
                  CREATE INDEX idx_object_tags on object USING GIN ("tags");
                  CREATE TABLE activity (
                      aid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      createtime TIMESTAMP,
                      jid        BIGINT,
                      module     TEXT,
                      args       TEXT
                  );
                  CREATE TABLE activity_trigger (
                      aid       BIGINT,
                      kind      TEXT,
                      tags      TEXT[]
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
                      xid       BIGINT,
                      event     TEXT,
                      data      TEXT
                  );
                  CREATE VIEW active_job AS
                      SELECT * from job 
                      WHERE (stoptime IS NULL) AND NOT (starttime IS NULL);
                  CREATE VIEW active_activity AS
                      SELECT activity.aid,activity.module,activity.jid FROM activity,active_job
                      WHERE active_job.jid = activity.jid;
                  CREATE VIEW active_activity_in AS
                      SELECT active_activity.aid,activation_in.oid,active_activity.jid FROM active_activity, activation, activation_in
                      WHERE active_activity.aid = activation.aid AND activation.avid = activation_in.avid;
                  CREATE VIEW activity_trigger_oid AS
                      SELECT activity.aid,object.oid,activity.jid FROM activity, activity_trigger, object
                      WHERE activity.aid = activity_trigger.aid AND activity_trigger.kind = object.kind AND activity_trigger.tags <@ object.tags;
                  CREATE VIEW objects_todo AS
                  SELECT * from activity_trigger_oid 
                  EXCEPT SELECT * from active_activity_in;
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
                  DROP TABLE IF EXISTS context    CASCADE;
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


    def add_context(self,name,description):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO context (name,description) VALUES (%s,%s);",[name,description])
            cur.execute("select last_value from combine_global_id;")
            cid = singlevalue(cur)
            self.conn.commit()
            return self.get_context(cid)
        except Exception as ex:
            handle_db_error("add_context",ex)

    def add_job(self,context,name,description):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO job (cid,name,description,createtime) VALUES (%s,%s,%s,clock_timestamp());",[context.cid(),name,description])
            cur.execute("select last_value from combine_global_id;")
            jid = singlevalue(cur)
            self.conn.commit()
            return self.get_job(jid)

        except Exception as ex:
            handle_db_error("add_job",ex)

    def add_activity(self,job,module,args,triggerseq):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO activity (createtime,jid,module,args) VALUES (clock_timestamp(),%s,%s,%s);",[job.jid(),module,args])
            cur.execute("select last_value from combine_global_id;")
            aid = singlevalue(cur)
            for trigger in triggerseq:
                cur.execute("INSERT INTO activity_trigger (aid,kind,tags) VALUES (%s,%s,%s);",[aid,trigger[0],trigger[1]])
            self.conn.commit()
            return self.get_activity(aid)
        except Exception as ex:
            handle_db_error("add_activity",ex)

    def add_activation(self,aid):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO activation(createtime,aid) VALUES (clock_timestamp(),%s);",[aid])
            cur.execute("select last_value from combine_global_id;")
            avid = singlevalue(cur)
            self.conn.commit()
            return self.get_activation(avid)
        except Exception as ex:
            handle_db_error("add_activation",ex)

    def add_object(self,job,activation,kind,tags,content_type,content):
        try:
            if activation is None:
                avid = 0
            else:
                avid = activation.avid()
            cur = self.conn.cursor()
            cur.execute("INSERT INTO object (time,jid,avid,kind,tags,content_type,content) VALUES (clock_timestamp(),%s,%s,%s,%s,%s,%s);",[job.jid(),avid,kind,tags,content_type,content])
            cur.execute("select last_value from combine_global_id;")
            oid = singlevalue(cur)
            self.conn.commit()
            return self.get_object(oid)
        except Exception as ex:
            handle_db_error("add_object",ex)

    def set_activation_graph(self,activation,inseq,outseq):
        try:
            cur = self.conn.cursor()
            for n in inseq:
                cur.execute("INSERT INTO activation_in  (avid,oid) VALUES (%s,%s);",[activation.avid(),n.oid()])
            for n in outseq:
                cur.execute("INSERT INTO activation_out  (avid,oid) VALUES (%s,%s);",[activation.avid(),n.oid()])
            self.conn.commit()
        except Exception as ex:
            handle_db_error("set_activation_graph",ex)

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

    def get_context(self,cid):
        return PgContext(self,cid)

    def get_job(self,jid):
        return PgJob(self,jid)

    def get_activity(self,aid):
        return PgActivity(self,aid)

    def get_activation(self,aid):
        return PgActivation(self,aid)


    def active_jobs(self):
        cur = self.conn.cursor()
        cur.execute("SELECT jid FROM active_job;")
        return [self.get_job(row[0]) for row in cur.fetchall()]

    def objects_todo(self,job):
        cur = self.conn.cursor()
        cur.execute("select aid, oid from objects_todo where jid="+str(job.jid())+" order by oid;")
        return [(row[0],row[1]) for row in cur.fetchall()]

def singlevalue(cur):
    # TODO check if it is really a single value
    if cur.rowcount == 1:
        rows = cur.fetchall()
        return rows[0][0]
    else:
        raise Exception('postgres result not a single value')

class PgDictWrapper:

   def __init__(self,db,sql_statement):
       self.db = db
       cur = db.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
       cur.execute(sql_statement)
       self.cacherec = cur.fetchone()

   def __getattr__(self, name):

       def _try_retrieve(*args, **kwargs):
           # logging.info("PgDictWrapper:retrieve dict attr:"+name)
           return self.cacherec[name]

       return _try_retrieve

class PgObject(PgDictWrapper):

   def __init__(self,db,idvalue):
       super(PgObject, self).__init__(db,"select * from object where oid="+str(idvalue)+";")

class PgActivity(PgDictWrapper):

   def __init__(self,db,idvalue):
       super(PgActivity, self).__init__(db,"select * from activity where aid="+str(idvalue)+";")
       self.activation = None
       self.job = None

   def start_activation(self):
       if not(self.activation is None):
           raise Exception("Start already started activation")
       self.activation = self.db.add_activation(self.aid())
       if self.job is None:
           self.job = self.db.get_job(self.jid())
       self.inobj  = []
       self.outobj = []

   def add2in(self,o):
       self.inobj.append(o)

   def add2out(self,o):
       self.outobj.append(o)

   def create_object(self,kind,tags,content_type,content):
        newobj = self.db.add_object(self.job,self.activation,kind,tags,content_type,content)
        return newobj

   def finish_activation(self):
       self.db.set_activation_graph(self.activation,self.inobj,self.outobj)
       self.activation = None

class PgActivation(PgDictWrapper):

   def __init__(self,db,idvalue):
       super(PgActivation, self).__init__(db,"select * from activation where avid="+str(idvalue)+";")

class PgContext(PgDictWrapper):

   def __init__(self,db,idvalue):
       super(PgContext, self).__init__(db,"select * from context where cid="+str(idvalue)+";")

class PgJob(PgDictWrapper):

   def __init__(self,db,idvalue):
       super(PgJob, self).__init__(db,"select * from job where jid="+str(idvalue)+";")
       self.idvalue = idvalue

   def start(self):
       try:
           cur = self.db.conn.cursor()
           cur.execute("UPDATE job SET starttime=clock_timestamp(), stoptime=NULL WHERE jid="+str(self.idvalue)+";")
           self.db.conn.commit()
       except Exception as ex:
           handle_db_error("job:start: ",ex)
        

def handle_db_error(what, ex):
    print(what+": "+str(ex), file=sys.stderr)
    traceback.print_exc(file=sys.stdout)
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

    pdb = postgres(conn)
    return pdb

