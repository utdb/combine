import sys
import traceback
import logging
import json
import configparser
import psycopg2
import psycopg2.extras


class PostgresConnection:

    def __init__(self, conn):
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
                      name              VARCHAR(32) UNIQUE,
                      description       TEXT
                  );

                  CREATE TABLE job (
                      jid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      cid               BIGINT,
                      name              VARCHAR(32) UNIQUE,
                      description       TEXT,
                      createtime        TIMESTAMP,
                      starttime         TIMESTAMP,
                      stoptime          TIMESTAMP,
                      seed              BIGINT[],
                      initialized       BOOLEAN DEFAULT FALSE
                  );

                  CREATE TABLE object (
                      oid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      jid          BIGINT,
                      time         TIMESTAMP,
                      avid         BIGINT,
                      metadata     JSONB,
                      kindtags     JSONB,
                      -- TODO, raw data must be BYTEA
                      raw_data     TEXT,
                      json_data    JSONB
                  );
                  CREATE INDEX okindginp ON object USING gin (kindtags);
                  -- CREATE INDEX okindginp ON object USING gin (jdoc jsonb_path_ops);
                  CREATE TABLE activity (
                      aid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      createtime TIMESTAMP,
                      jid        BIGINT,
                      module     TEXT,
                      args       TEXT
                  );
                  CREATE TABLE activity_trigger (
                      aid       BIGINT,
                      kindtags  JSONB
                  );
                  CREATE INDEX atkindginp ON activity_trigger USING gin (kindtags);
                  CREATE TABLE activation (
                      avid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      createtime  TIMESTAMP,
                      aid         BIGINT,
                      oid_in      BIGINT[],
                      oid_out     BIGINT[],
                      status      CHAR
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
                      SELECT activity.aid, activity.module, activity.jid FROM activity, active_job
                      WHERE active_job.jid = activity.jid;
                  -- CREATE VIEW active_activity_in AS
                      -- SELECT active_activity.aid, activation_in.oid, active_activity.jid FROM active_activity,  activation,  activation_in
                      -- WHERE active_activity.aid = activation.aid AND activation.avid = activation_in.avid;
                  -- CREATE VIEW activity_trigger_oid AS
                      -- SELECT activity.aid, object.oid, activity.jid FROM activity,  activity_trigger,  object
                      -- WHERE activity.aid = activity_trigger.aid AND activity_trigger.kind = object.kind AND activity_trigger.tags <@ object.tags;
                  -- CREATE VIEW objects_todo AS
                  -- SELECT * from activity_trigger_oid
                  -- EXCEPT SELECT * from active_activity_in;
                  CREATE VIEW activity_objects AS
                  SELECT activity.module,activity.jid,activation.avid,object.oid
                  FROM activity,activation,object 
                  WHERE activity.aid = activation.aid AND activation.avid = object.avid;
                  -- CREATE VIEW avinout AS
                  -- SELECT activation_in.avid, activation_in.oid AS oid_in, activation_out.oid as oid_out
                  -- FROM activation_in, activation_out
                  -- WHERE activation_in.avid = activation_out.avid;
               """
            cur.execute(stat)
            self.conn.commit()
        except Exception as ex:
            handle_db_error("create", ex)

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
            handle_db_error("destroy", ex)

    def add_context(self, name, description):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO context (name, description) VALUES (%s, %s);", [name, description])
            cur.execute("select last_value from combine_global_id;")
            cid = singlevalue(cur)
            self.conn.commit()
            return self.get_context(cid)
        except Exception as ex:
            handle_db_error("add_context", ex)

    def add_job(self, context, name, description):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO job (cid, name, description, createtime) VALUES (%s, %s, %s, clock_timestamp());", [context.cid(), name, description])
            cur.execute("select last_value from combine_global_id;")
            jid = singlevalue(cur)
            self.conn.commit()
            return self.get_job(jid=jid)

        except Exception as ex:
            handle_db_error("add_job", ex)

    def add_activity(self, job, module, args, triggerseq):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO activity (createtime, jid, module, args) VALUES (clock_timestamp(), %s, %s, %s);", [job.jid(), module, args])
            cur.execute("select last_value from combine_global_id;")
            aid = singlevalue(cur)
            for trigger in triggerseq:
                cur.execute("INSERT INTO activity_trigger (aid, kindtags) VALUES (%s, %s);", [aid, json.dumps(trigger)])
            self.conn.commit()
            return self.get_activity(aid)
        except Exception as ex:
            handle_db_error("add_activity", ex)

    def add_seed_data(self, job, objects):
        try:
            seed = []
            for obj in objects:
                if obj.lightweight():
                    newobj = self.create_object(job, None, obj.kindtags(), obj.metadata(), obj.raw_data(), obj.json_data(), commit=False)
                else:
                    newobj = obj
                    print("add_seed_data: Unexpected Object: "+str(obj))
            seed.append(newobj.oid())
            cur = self.conn.cursor()
            cur.execute("UPDATE job SET seed=%s WHERE jid=%s ;",[seed,job.jid()])
            self.conn.commit()
        except Exception as ex:
            handle_db_error("add_seed_data", ex)

    def add_activation(self, aid):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO activation(createtime, aid, status) VALUES (clock_timestamp(), %s, %s);", [aid, 's'])
            cur.execute("select last_value from combine_global_id;")
            avid = singlevalue(cur)
            self.conn.commit()
            return self.get_activation(avid)
        except Exception as ex:
            handle_db_error("add_activation", ex)

    def create_object(self, job, activation, kindtags, metadata, raw_data, json_data, commit=True):
        try:
            if activation is None:
                avid = 0
            else:
                avid = activation.avid()
            cur = self.conn.cursor()
            cur.execute("INSERT INTO object (time, jid, avid, kindtags, metadata, raw_data, json_data) VALUES (clock_timestamp(), %s, %s, %s, %s, %s, %s);", [job.jid(), avid, json.dumps(kindtags), json.dumps(metadata), raw_data, json.dumps(json_data)])
            cur.execute("select last_value from combine_global_id;")
            oid = singlevalue(cur)
            if commit:
                self.conn.commit()
            return self.get_object(oid)
        except Exception as ex:
            handle_db_error("add_object", ex)

    def set_activation_graph(self, activation, obj_in, obj_out):
        try:
            cur = self.conn.cursor()
            oid_in = [ obj.oid() for obj in obj_in]
            oid_out = [ obj.oid() for obj in obj_out]
            cur.execute("UPDATE activation SET oid_in=%s, oid_out=%s WHERE avid=%s;",[oid_in, oid_out, activation.avid()])
            self.conn.commit()
        except Exception as ex:
            handle_db_error("set_activation_graph", ex)

    def add_log(self, xid, event, data):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO log (time, xid, event, data) VALUES (clock_timestamp(), %s, %s, %s);", [xid, event, data])
            cur.execute("select last_value from combine_global_id;")
            lid = singlevalue(cur)
            self.conn.commit()
            return lid
        except Exception as ex:
            handle_db_error("add_log", ex)

    def get_object(self, oid):
        return PgObject(self, oid)

    def get_context(self, cid):
        return PgContext(self, cid)

    def get_job(self, jid=None, name=None):
        return PgJob(self, jid, name)

    def get_activity(self, aid):
        return PgActivity(self, aid)

    def get_activation(self, aid):
        return PgActivation(self, aid)

    def active_jobs(self):
        cur = self.conn.cursor()
        cur.execute("SELECT jid FROM active_job;")
        return [self.get_job(jid=row[0]) for row in cur.fetchall()]

    def objects_todo(self, job):
        cur = self.conn.cursor()
        cur.execute("SELECT DISTINCT ON (oid) oid, aid from objects_todo where jid="+str(job.jid())+" order by oid;")
        return [(row[1], row[0]) for row in cur.fetchall()]


def singlevalue(cur):
    # TODO check if it is really a single value
    if cur.rowcount == 1:
        rows = cur.fetchall()
        return rows[0][0]
    else:
        raise Exception('postgres result not a single value')


class PgDictWrapper:

    def __init__(self, db, sql_statement):
        self.db = db
        cur = db.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql_statement)
        self.cacherec = cur.fetchone()

    def __getattr__(self,  name):

        def _try_retrieve(*args,  **kwargs):
            # logging.info("PgDictWrapper:retrieve dict attr:"+name)
            return self.cacherec[name]

        return _try_retrieve


class PgObject(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgObject,  self).__init__(db, "select * from object where oid="+str(idvalue)+";")

    def lightweight(self):
        return False

class PgActivity(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgActivity,  self).__init__(db, "select * from activity where aid="+str(idvalue)+";")
        self.trigger = []
        cur = db.conn.cursor()
        cur.execute("select aid, kindtags from activity_trigger where aid="+str(idvalue)+";")
        for row in cur.fetchall():
            self.trigger.append(row[1])

    def activity_triggers(self):
        cur = self.db.conn.cursor()
        # TODO: remove object from next query, hunt missing tuples
        cur.execute('SELECT kindtags FROM activity_trigger WHERE aid = %s;',[self.aid(),])
        rows = cur.fetchall()
        self.db.conn.commit()
        for row in rows:
            yield [(row[0])['kind'], set((row[0])['tags'])]

    def objects_in(self):
        cur = self.db.conn.cursor()
        # TODO: remove object from next query, hunt missing tuples
        cur.execute('SELECT activation_in.oid FROM activation, activation_in, object WHERE activation.aid=%s AND activation.avid = activation_in.avid AND activation_in.oid=object.oid;',[self.aid(),])
        rows = cur.fetchall()
        self.db.conn.commit()
        for row in rows:
            yield self.db.get_object(row[0])

    def objects_out(self):
        cur = self.db.conn.cursor()
        cur.execute('SELECT oid FROM activity_objects where jid=%s AND module=%s;',[self.jid(),self.module()])
        rows = cur.fetchall()
        self.db.conn.commit()
        for row in rows:
            yield self.db.get_object(row[0])



class PgActivityTrigger(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgActivityTrigger,  self).__init__(db, "select * from activity_trigger where aid="+str(idvalue)+";")


class PgActivation(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgActivation,  self).__init__(db, "select * from activation where avid="+str(idvalue)+";")
        self.idvalue = idvalue

    def set_status(self, status):
        try:
            cur = self.db.conn.cursor()
            cur.execute("UPDATE activation SET status=\'"+status+"\' WHERE avid="+str(self.idvalue)+";")
            self.db.conn.commit()
        except Exception as ex:
            handle_db_error("job:start: ", ex)


class PgContext(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgContext,  self).__init__(db, "select * from context where cid="+str(idvalue)+";")


class PgJob(PgDictWrapper):

    def __init__(self, db, idvalue=None, name=None):
        super(PgJob,  self).__init__(db, 'select * from job where ' +
                (('jid='+str(idvalue)) if idvalue is not None else ('name=\''+name+'\'')) +";")
        self.idvalue = self.jid()

    def start(self):
        try:
            cur = self.db.conn.cursor()
            cur.execute("UPDATE job SET starttime=clock_timestamp(),  stoptime=NULL WHERE jid="+str(self.idvalue)+";")
            self.db.conn.commit()
        except Exception as ex:
            handle_db_error("job:start: ", ex)

    def activities(self):
        cur = self.db.conn.cursor()
        cur.execute('SELECT aid FROM activity where jid=%s;',[self.jid(), ])
        rows = cur.fetchall()
        self.db.conn.commit()
        for row in rows:
            yield self.db.get_activity(row[0])

    def delete_objects(self, activity=None):
        cur = self.db.conn.cursor()
        if activity is None:
            cur.xecute('SELECT avid,oid INTO TEMPORARY delobj_base FROM activity_objects where jid=%s ;',[self.jid()])
        else:
            cur.execute('SELECT avid,oid INTO TEMPORARY delobj_base FROM activity_objects where jid=%s AND module=%s;',[self.jid(),activity])
        recursive_stat = """
            WITH RECURSIVE avid_oid(avid,oid) AS (
                SELECT * from delobj_base
            UNION ALL
                SELECT delta.avid, delta.oid_out
                FROM avid_oid base, avinout delta
                WHERE base.oid = delta.oid_in
                )
            SELECT * INTO TEMPORARY delobj FROM avid_oid;
        """
        cur.execute(recursive_stat)
        cur.execute('DELETE FROM object WHERE oid IN (select oid from delobj);')
        cur.execute('DELETE FROM activation_in WHERE avid IN (select avid from delobj);')
        cur.execute('DELETE FROM activation_out WHERE avid IN (select avid from delobj);')
        cur.execute('DELETE FROM activation WHERE avid IN (select avid from delobj);')
        self.db.conn.commit()


def handle_db_error(what,  ex):
    print(what+": "+str(ex),  file=sys.stderr)
    traceback.print_exc(file=sys.stdout)
    sys.exit()


def opendb(configfile):
    """
    Initialize a postgres connection
    """
    config = configparser.RawConfigParser()
    config.read(configfile)
    try:
        conn = psycopg2.connect("dbname='"+config.get("postgres",  "db")+"' user='"+config.get("postgres",  "user")+"' host='"+config.get("postgres",  "host")+"' password='"+config.get("postgres",  "password")+"'")
    except Exception as ex:
        handle_db_error("Unable to connect to postgres"+str(ex))

    pdb = PostgresConnection(conn)
    return pdb
