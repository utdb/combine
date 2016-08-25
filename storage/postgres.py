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
        self.conn.commit()
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

                  CREATE TABLE resource (
                      rid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      jid               BIGINT,
                      label             TEXT UNIQUE,
                      bytes_data        BYTEA
                  );
                  CREATE INDEX resource_label ON resource USING hash (label);

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
                      bytes_data   BYTEA,
                      json_data    JSONB
                  );
                  CREATE INDEX okindginp ON object USING gin (kindtags);
                  -- CREATE INDEX okindginp ON object USING gin (jdoc jsonb_path_ops);
                  CREATE TABLE activity (
                      aid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      createtime TIMESTAMP,
                      jid        BIGINT,
                      module     TEXT,
                      args       TEXT,
                      kindtags_out JSONB,
                      stateless  BOOLEAN DEFAULT TRUE
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
                      rsrc_in     BIGINT[],
                      rsrc_out    BIGINT[],
                      status      CHAR
                  );
                  CREATE TABLE log (
                      lid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                      time      TIMESTAMP,
                      event     TEXT,
                      message   JSONB
                  );
                  -- select avid, oid from activation, unnest(oid_out) oid  WHERE 58=ANY(oid_out);

                  CREATE VIEW active_job AS
                      SELECT * from job
                      WHERE (stoptime IS NULL) AND NOT (starttime IS NULL);
                  CREATE VIEW active_activity AS
                      SELECT activity.aid, activity.module, activity.jid FROM activity, active_job
                      WHERE active_job.jid = activity.jid;
                  CREATE VIEW active_activity_in AS
                      SELECT active_activity.aid, oid, active_activity.jid FROM active_activity,  activation, unnest(activation.oid_in) oid
                      WHERE active_activity.aid = activation.aid;
                  CREATE VIEW activity_trigger_oid AS
                      SELECT activity.aid, object.oid, activity.jid FROM activity,  activity_trigger,  object
                      WHERE activity.aid = activity_trigger.aid AND text(activity_trigger.kindtags::json->'kind') = text(object.kindtags::json->'kind');
                  -- INCOMPLETE TODO ALSO <@ on the tags
                  CREATE VIEW objects_todo AS
                  SELECT * from activity_trigger_oid
                  EXCEPT SELECT * from active_activity_in;
                  CREATE VIEW activity_objects AS
                      SELECT activity.module,activity.jid,activation.avid,object.oid
                      FROM activity,activation,object
                      WHERE activity.aid = activation.aid AND activation.avid = object.avid;
                  --
                  CREATE VIEW avinout AS
                      SELECT avid, oidin, oidout
                      FROM activation, unnest(activation.oid_in) oidin, unnest(activation.oid_out) oidout;
                  CREATE VIEW activity_in AS
                      SELECT activity.aid, activation.avid, oid
                      FROM activity, activation, unnest(activation.oid_in) oid
                      WHERE activity.aid = activation.aid;
                  CREATE VIEW activity_out AS
                      SELECT activity.aid, activation.avid, oid
                      FROM activity, activation, unnest(activation.oid_out) oid
                      WHERE activity.aid = activation.aid;
                  CREATE VIEW activation_rsrc_in AS
                      SELECT activation.aid, activation.avid, rid
                      FROM activation, unnest(activation.rsrc_in) rid;
                  CREATE VIEW activation_rsrc_out AS
                      SELECT activation.avid, rid
                      FROM activation, unnest(activation.rsrc_out) rid;
                  CREATE RECURSIVE VIEW activity_out_all (aid, avid, oid) AS
                      select aid, avid, oid from activity_out
                  UNION ALL
                      SELECT base.aid, delta.avid, delta.oidout
                      FROM activity_out_all base, avinout delta
                      WHERE base.oid = delta.oidin;
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
                  DROP TABLE IF EXISTS resource    CASCADE;
                  DROP TABLE IF EXISTS job    CASCADE;
                  DROP TABLE IF EXISTS object     CASCADE;
                  DROP TABLE IF EXISTS actions    CASCADE;
                  DROP TABLE IF EXISTS provenance CASCADE;
                  DROP TABLE IF EXISTS activity    CASCADE;
                  DROP TABLE IF EXISTS activity_trigger    CASCADE;
                  DROP TABLE IF EXISTS activation    CASCADE;
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

    def add_resource(self, label):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO resource (label) VALUES (%s);", [label])
            cur.execute("select last_value from combine_global_id;")
            rid = singlevalue(cur)
            self.conn.commit()
            return self.get_resource(label=label)

        except Exception as ex:
            handle_db_error("add_job", ex)

    def add_job(self, context, name, description):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO job (cid, name, description, createtime) VALUES (%s, %s, %s, clock_timestamp());", [context.cid, name, description])
            cur.execute("select last_value from combine_global_id;")
            jid = singlevalue(cur)
            self.conn.commit()
            return self.get_job(jid=jid)

        except Exception as ex:
            handle_db_error("add_job", ex)

    def add_activity(self, job, module, args, kindtags_in, kindtags_out, stateless=True):
        add_missing_tags(kindtags_in)
        add_missing_tags(kindtags_out)
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO activity (createtime, jid, module, args, kindtags_out, stateless) VALUES (clock_timestamp(), %s, %s, %s, %s, %s);", [job.jid, module, args, json.dumps(kindtags_out), stateless])
            cur.execute("select last_value from combine_global_id;")
            aid = singlevalue(cur)
            for trigger in kindtags_in:
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
                    newobj = self.create_object(job, None, obj.kindtags(), obj.metadata(), obj.str_data(), obj.bytes_data(), obj.json_data(), commit=False)
                else:
                    newobj = obj
                    print("add_seed_data: Unexpected Object: "+str(obj))
            seed.append(newobj.oid)
            cur = self.conn.cursor()
            cur.execute("UPDATE job SET seed=%s WHERE jid=%s ;", [seed, job.jid])
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

    def create_object(self, job, activation, kindtags, metadata, str_data, bytes_data, json_data, commit=True):
        if str_data is not None:
            if bytes_data is not None:
                raise Exception('create_object: str_data and bytes_data cannot have value at same time')
            bytes_data = str_data.encode('utf-8')
        try:
            if 'tags' not in kindtags:
                kindtags['tags'] = []
            if activation is None:
                avid = 0
            else:
                avid = activation.avid
            cur = self.conn.cursor()
            cur.execute("INSERT INTO object (time, jid, avid, kindtags, metadata, bytes_data, json_data) VALUES (clock_timestamp(), %s, %s, %s, %s, %s, %s);", [job.jid, avid, json.dumps(kindtags), json.dumps(metadata), psycopg2.Binary(bytes_data), json.dumps(json_data)])
            cur.execute("select last_value from combine_global_id;")
            oid = singlevalue(cur)
            if commit:
                self.conn.commit()
            return self.get_object(oid)
        except Exception as ex:
            handle_db_error("create_object", ex)

    def set_activation_graph(self, activation, obj_in, obj_out, rsrc_in= None, rsrc_out= None):
        try:
            cur = self.conn.cursor()
            oid_in = [obj.oid for obj in obj_in]
            oid_out = [obj.oid for obj in obj_out]
            #
            if rsrc_in is not None:
                rid_in = [rsrc.rid for rsrc in rsrc_in]
            else:
                rid_in = None
            if rsrc_out is not None:
                rid_out = [rsrc.rid for rsrc in rsrc_out]
            else:
                rid_out = None
            #
            #
            cur.execute("UPDATE activation SET oid_in=%s, oid_out=%s, rsrc_in=%s, rsrc_out=%s WHERE avid=%s;", [oid_in, oid_out, rid_in, rid_out, activation.avid])
            self.conn.commit()
        except Exception as ex:
            handle_db_error("set_activation_graph", ex)

    def add_log(self, event, message):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO log (time, event, message) VALUES (clock_timestamp(), %s, %s);", [event, json.dumps(message)])
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

    def get_resource(self, label, create= False):
        try:
            res = PgResource(self, label)
        except psycopg2.Error as e:
            if create:
                res = self.add_resource(label)
            else:
                handle_db_error("get_resourceg", str(e))
        return res

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
        cur.execute("SELECT DISTINCT ON (oid) oid, aid from objects_todo where jid="+str(job.jid)+" order by oid;")
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
        self._db = db
        cur = self._db.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql_statement)
        self.cacherec = cur.fetchone()
        if self.cacherec is None:
            raise psycopg2.Error('Object not found: ' + sql_statement)

    # now a non function to make Brend happy
    def __getattr__(self,  name):
        return self.cacherec[name]

    # incomplete, does not work for internal attr
    # def __setattr__(self, name, value):
        # raise psycopg2.Error('NOT IMPLEMENTED')
        # return self.cacherec[name]


class PgObject(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgObject,  self).__init__(db, "select * from object where oid="+str(idvalue)+";")

    def lightweight(self):
        return False

    def str_data(self):
        res = self.bytes_data
        if res is not None:
            res = bytes(res).decode('utf-8')
        return res

class PgActivity(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgActivity,  self).__init__(db, "select * from activity where aid="+str(idvalue)+";")
        self.trigger = []
        cur = self._db.conn.cursor()
        cur.execute("select aid, kindtags from activity_trigger where aid="+str(idvalue)+";")
        for row in cur.fetchall():
            self.trigger.append(row[1])

    def activity_triggers(self):
        cur = self._db.conn.cursor()
        # TODO: remove object from next query, hunt missing tuples
        cur.execute('SELECT kindtags FROM activity_trigger WHERE aid = %s;', [self.aid, ])
        rows = cur.fetchall()
        self._db.conn.commit()
        for row in rows:
            yield [(row[0])['kind'], set((row[0])['tags'])]

    def activity_objects(self, view, commit):
        cur = self._db.conn.cursor()
        # print('SELECT oid FROM '+view+' WHERE aid = ' + str(self.aid) + ';')
        cur.execute('SELECT oid FROM '+view+' WHERE aid = ' + str(self.aid) + ';')
        rows = cur.fetchall()
        if commit:
            self._db.conn.commit()
        for row in rows:
            yield self._db.get_object(row[0])

    def objects_in(self, commit=False):
        return self.activity_objects('activity_in', commit)

    def objects_out(self, commit=False):
        return self.activity_objects('activity_out', commit)

    def objects_out_all(self, commit=False):
        return self.activity_objects('activity_out_all', commit)

    def activity_oids(self, view, commit):
        self._db.conn.commit()
        cur = self._db.conn.cursor()
        cur.execute('SELECT oid FROM '+view+' WHERE aid = ' + str(self.aid) + ';')
        rows = cur.fetchall()
        if commit:
            self._db.conn.commit()
        oid_seq = [row[0] for row in rows]
        return oid_seq

    def oids_in(self, commit=False):
        return self.activity_oids('activity_in', commit)

    def oids_out(self, commit=False):
        return self.activity_oids('activity_out', commit)

    def oids_triggered(self, commit=False):
        return self.activity_oids('activity_trigger_oid', commit)


class PgActivityTrigger(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgActivityTrigger,  self).__init__(db, "select * from activity_trigger where aid="+str(idvalue)+";")


class PgActivation(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgActivation,  self).__init__(db, "select * from activation where avid="+str(idvalue)+";")
        self.idvalue = idvalue

    def set_status(self, status):
        try:
            cur = self._db.conn.cursor()
            cur.execute("UPDATE activation SET status=\'"+status+"\' WHERE avid="+str(self.idvalue)+";")
            self._db.conn.commit()
        except Exception as ex:
            handle_db_error("job:start: ", ex)


class PgContext(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgContext,  self).__init__(db, "select * from context where cid="+str(idvalue)+";")


class PgResource(PgDictWrapper):

    def __init__(self, db, label):
        super(PgResource,  self).__init__(db, "select * from resource where label=\'"+label+"\';")


class PgJob(PgDictWrapper):

    def __init__(self, db, idvalue=None, name=None):
        super(PgJob,  self).__init__(db,
                'select * from job where ' +
                (('jid=' + str(idvalue)) if idvalue is not None else ('name=\''+name+'\'')) + ";")
        self.idvalue = self.jid

    def start(self):
        try:
            cur = self._db.conn.cursor()
            cur.execute("UPDATE job SET starttime=clock_timestamp(),  stoptime=NULL WHERE jid="+str(self.idvalue)+";")
            self._db.conn.commit()
        except Exception as ex:
            handle_db_error("job:start: ", ex)

    def set_initialized(self):
        try:
            cur = self._db.conn.cursor()
            cur.execute("UPDATE job SET initialized=TRUE WHERE jid="+str(self.idvalue)+";")
            self._db.conn.commit()
        except Exception as ex:
            handle_db_error("job:start: ", ex)

    def activities(self, module=None):
        cur = self._db.conn.cursor()
        if module is None:
            cur.execute('SELECT aid FROM activity where jid=%s;', [self.jid, ])
        else:
            cur.execute('SELECT aid FROM activity where jid=%s AND activity.module = %s;', [self.jid, module])
        rows = cur.fetchall()
        self._db.conn.commit()
        for row in rows:
            yield self._db.get_activity(row[0])

    def delete_objects(self, activity):
        cur = self._db.conn.cursor()
        cur.execute("SELECT * INTO TEMPORARY delobj FROM activity_out_all WHERE aid = %s;", [activity.aid])
        cur.execute('DELETE FROM object WHERE oid IN (select oid from delobj);')
        cur.execute('DELETE FROM activation WHERE avid IN (select distinct avid from delobj);')
        self._db.conn.commit()


def handle_db_error(what,  ex):
    print(what+": "+str(ex),  file=sys.stderr)
    traceback.print_exc(file=sys.stdout)
    sys.exit()

def add_missing_tags(kindtags_seq):
    for kindtags in kindtags_seq:
        if 'tags' not in kindtags:
            kindtags['tags'] = []


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
