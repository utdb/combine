import sys
from threading import Thread
import traceback
import logging
import json
import configparser
import psycopg2
import psycopg2.extras


class PostgresConnection:

    def __init__(self, db_config):
        self.db_config = db_config
        self.last_error = None
        self.reconnect()

    def reconnect(self):
        logging.info("Open Postgres DB: "+str(self.db_config))
        self.max_logs = 1  # always flush for order at the moment
        self.log_list = []
        self.conn = psycopg2.connect(**self.db_config)

    def rollback(self):
        try:
            self.conn.rollback()
            self.conn = psycopg2.connect(**self.db_config)
        except:
            # ignore
            print("ERROR DURING ROLLBACK")
            self.conn = None

    def commit(self):
        self.conn.commit()

    def close(self):
        try:
            if self.conn is not None:
                self._flush_log()
                self.conn.close()
                logging.info("Close Postgres DB: "+str(self.conn))
        except psycopg2.Error as e:
            print('Warning Postgres closing Error:\n'+str(e), file=sys.stderr)
        self.conn = None

    def create(self):
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
                  initialized       BOOLEAN DEFAULT FALSE,
                  version_id        BIGINT
              );

              CREATE TABLE object (
                  oid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                  jid          BIGINT,
                  time         TIMESTAMP,
                  avid         BIGINT,
                  metadata     JSONB,
                  kindtags     JSONB,
                  bytes_data   BYTEA,
                  json_data    JSONB,
                  sentence     TEXT
              );
              CREATE INDEX okindginp ON object USING gin (kindtags);

              CREATE TABLE activity (
                  aid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                  createtime TIMESTAMP,
                  jid        BIGINT,
                  module     TEXT,
                  args       TEXT,
                  kindtags_out JSONB,
                  stateless  BOOLEAN DEFAULT TRUE,
                  initialized BOOLEAN DEFAULT FALSE
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
                  rid_in      BIGINT[],
                  rid_out     BIGINT[],
                  status      CHAR
              );
              CREATE TABLE log (
                  lid BIGINT PRIMARY KEY DEFAULT nextval('combine_global_id'),
                  time      TIMESTAMP,
                  event     TEXT,
                  message   JSONB
              );
              -- CREATE TABLE pagecache (
                  -- url       TEXT PRIMARY KEY,
                  -- content   TEXT
              -- );
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
              CREATE VIEW activity_in AS
                  SELECT activity.aid, activation.avid, oid
                  FROM activity, activation, unnest(activation.oid_in) oid
                  WHERE activity.aid = activation.aid;
              CREATE VIEW activity_out AS
                  SELECT activity.aid, activation.avid, oid
                  FROM activity, activation, unnest(activation.oid_out) oid
                  WHERE activity.aid = activation.aid;
              CREATE OR REPLACE VIEW activation_graph AS
                  SELECT parent.aid, parent.avid AS avid_parent, child.avid AS avid_child
                  FROM activation AS parent, activation AS child
                  WHERE (parent.oid_out && child.oid_in) OR (parent.rid_out && child.rid_in) AND parent.avid != child.avid;
              CREATE OR REPLACE RECURSIVE VIEW all_avid_descendants (aid, avid) AS
                  SELECT aid, avid from activation
              UNION DISTINCT
                  SELECT base.aid, graph.avid_child AS avid
                  FROM all_avid_descendants base, activation_graph graph
                  WHERE base.avid = graph.avid_parent;

           """
        cur.execute(stat)

    def destroy(self):
        # TODO during development just empty the database
        # drop schema public cascade;
        # create schema public;
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
              -- DROP TABLE IF EXISTS pagecache        CASCADE;
              DROP SEQUENCE IF EXISTS combine_global_id CASCADE;
           """
        cur.execute(stat)

    def add_context(self, name, description):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO context (name, description) VALUES (%s, %s) RETURNING cid;", [name, description])
        cid = singlevalue(cur)
        self.add_log('context.create', {'cid': cid, 'name': name})
        return self.get_context_byid(cid)

    def get_context(self, name):
        cur = self.conn.cursor()
        cur.execute("SELECT cid from context WHERE name = %s;", [name, ])
        cid = singlevalue(cur)
        return self.get_context_byid(cid)

    def get_job(self, name):
        cur = self.conn.cursor()
        cur.execute("SELECT jid from job WHERE name = %s;", [name, ])
        jid = singlevalue(cur)
        return self.get_job_byid(jid)

    def get_page(self, url):
        cur = self.conn.cursor()
        cur.execute("SELECT content from pagecache WHERE url = %s;", [url, ])
        if cur.rowcount == 1:
            return singlevalue(cur)
        else:
            return None

    def put_page(self, url, content):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO pagecache (url, content) VALUES (%s, %s);", [url,content])

    def add_resource(self, label):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO resource (label) VALUES (%s) RETURNING rid;", [label])
        rid = singlevalue(cur)
        return self.get_resource(label=label)

    def add_job(self, context, name, description):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO job (cid, name, description, createtime, version_id) VALUES (%s, %s, %s, clock_timestamp(), 1001) RETURNING jid;", [context.cid, name, description])
        jid = singlevalue(cur)
        self.add_log('job.create', {'jid': jid, 'cid': context.cid, 'name': name})
        return self.get_job_byid(jid=jid)

    def add_activation(self, aid):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO activation(createtime, aid, status) VALUES (clock_timestamp(), %s, %s) RETURNING avid;", [aid, 's'])
        avid = singlevalue(cur)
        return self.get_activation(avid)

    def create_object(self, job, activation, kindtags, metadata, str_data, bytes_data, json_data, sentence):
        if str_data is not None:
            if bytes_data is not None:
                raise Exception('create_object: str_data and bytes_data cannot have value at same time')
        bytes_data = str_data.encode('utf-8')
        if 'tags' not in kindtags:
            kindtags['tags'] = []
        if activation is None:
            avid = 0
        else:
            avid = activation.avid
        cur = self.conn.cursor()
        cur.execute("INSERT INTO object (time, jid, avid, kindtags, metadata, bytes_data, json_data, sentence) VALUES (clock_timestamp(), %s, %s, %s, %s, %s, %s, %s) RETURNING oid;", [job.jid, avid, json.dumps(kindtags), json.dumps(metadata), psycopg2.Binary(bytes_data), json.dumps(json_data), sentence])
        oid = singlevalue(cur)
        return self.get_object(oid)

    def set_activation_graph(self, activation, obj_in, obj_out, rsrc_in=None, rsrc_out=None):
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
        cur.execute("UPDATE activation SET oid_in=%s, oid_out=%s, rid_in=%s, rid_out=%s WHERE avid=%s;", [oid_in, oid_out, rid_in, rid_out, activation.avid])

    def log_messages(self):
        cur = self.conn.cursor()
        cur.execute('SELECT lid, time, event, message  FROM log;')
        rows = cur.fetchall()
        for row in rows:
            yield [row[0], row[1], row[2], row[3]]

    # add a list of log messages [event,message], returns 'lid' of last message
    def _flush_log(self):
        lid = 0
        cur = self.conn.cursor()
        for log in self.log_list:
            message = log[1]
            cur.execute("INSERT INTO log (time, event, message) VALUES (clock_timestamp(), %s, %s) RETURNING lid;", [log[0], json.dumps(message)])
            lid = singlevalue(cur)
            message['event'] = log[0]
            cur.execute("NOTIFY new_log, %s;", [json.dumps(message)])
        self.log_list = []
        return lid

    def add_log(self, event, message, flush=False):
        self.log_list.append([event, message])
        if flush or len(self.log_list) >= self.max_logs:
            return self._flush_log()
        else:
            return None

    def force_log_message(self):
        try:
            if self.last_error is not None:
                print('Write Postgres error log message:\n' + json.dumps(self.last_error), file=sys.stderr)
                self.add_log("ERROR", self.last_error)
                self._flush_log()
                self.commit()
        except:
            None

    def get_object(self, oid):
        return PgObject(self, oid)

    def get_context_byid(self, cid):
        return PgContext(self, cid)

    def get_resource(self, label, create=False):
        try:
            # incomplete
            res = PgResource(self, label)
        except psycopg2.Error as e:
            if create:
                res = self.add_resource(label)
            else:
                handle_db_error("get_resourceg", str(e))
        return res

    def get_job_byid(self, jid=None, name=None):
        return PgJob(self, jid, name)

    def get_activity(self, aid):
        return PgActivity(self, aid)

    def get_activation(self, aid):
        return PgActivation(self, aid)

    def active_jobs(self):
        cur = self.conn.cursor()
        cur.execute("SELECT jid FROM active_job;")
        return [self.get_job_byid(jid=row[0]) for row in cur.fetchall()]

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

    def set_initialized(self):
        cur = self._db.conn.cursor()
        cur.execute("UPDATE activity SET initialized=TRUE WHERE aid="+str(self.aid)+";")

    def activity_triggers(self):
        cur = self._db.conn.cursor()
        cur.execute('SELECT kindtags FROM activity_trigger WHERE aid = %s;', [self.aid, ])
        rows = cur.fetchall()
        for row in rows:
            yield [(row[0])['kind'], set((row[0])['tags'])]

    def activity_objects(self, view):
        cur = self._db.conn.cursor()
        cur.execute('SELECT oid FROM '+view+' WHERE aid = ' + str(self.aid) + ';')
        rows = cur.fetchall()
        for row in rows:
            yield self._db.get_object(row[0])

    def objects_in(self):
        return self.activity_objects('activity_in')

    def objects_out(self):
        return self.activity_objects('activity_out')

    def objects_out_all(self):
        return self.activity_objects('activity_out_all')

    def activity_oids(self, view):
        cur = self._db.conn.cursor()
        cur.execute('SELECT oid FROM '+view+' WHERE aid = ' + str(self.aid) + ';')
        rows = cur.fetchall()
        oid_seq = [row[0] for row in rows]
        return oid_seq

    def oids_in(self):
        return self.activity_oids('activity_in')

    def oids_out(self):
        return self.activity_oids('activity_out')

    def oids_triggered(self):
        return self.activity_oids('activity_trigger_oid')


class PgActivityTrigger(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgActivityTrigger,  self).__init__(db, "select * from activity_trigger where aid="+str(idvalue)+";")


class PgActivation(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgActivation,  self).__init__(db, "select * from activation where avid="+str(idvalue)+";")
        self.idvalue = idvalue

    def set_status(self, status):
        cur = self._db.conn.cursor()
        cur.execute("UPDATE activation SET status=\'"+status+"\' WHERE avid="+str(self.idvalue)+";")


class PgContext(PgDictWrapper):

    def __init__(self, db, idvalue):
        super(PgContext,  self).__init__(db, "select * from context where cid="+str(idvalue)+";")


class PgResource(PgDictWrapper):

    def __init__(self, db, label):
        super(PgResource,  self).__init__(db, "select * from resource where label=\'"+label+"\';")


class PgJob(PgDictWrapper):

    def __init__(self, db, idvalue=None, name=None):
        super(PgJob,  self).__init__(db, 'select * from job where ' + (('jid=' + str(idvalue)) if idvalue is not None else ('name=\''+name+'\'')) + ";")
        self.idvalue = self.jid

    def start(self):
        cur = self._db.conn.cursor()
        cur.execute("UPDATE job SET starttime=clock_timestamp(),  stoptime=NULL WHERE jid="+str(self.idvalue)+";")

    def set_initialized(self):
        cur = self._db.conn.cursor()
        cur.execute("UPDATE job SET initialized=TRUE WHERE jid="+str(self.idvalue)+";")

    def new_version(self):
        new_version_id = this.version_id + 1
        cur = self._db.conn.cursor()
        cur.execute("UPDATE job SET version_id="+str(new_version_id)+" WHERE jid="+str(self.idvalue)+";")
        return new_version_id

    def activities(self, module=None):
        cur = self._db.conn.cursor()
        if module is None:
            cur.execute('SELECT aid FROM activity where jid=%s;', [self.jid, ])
        else:
            cur.execute('SELECT aid FROM activity where jid=%s AND activity.module = %s;', [self.jid, module])
        rows = cur.fetchall()
        for row in rows:
            yield self._db.get_activity(row[0])

    def delete_objects(self, activity):
        cur = self._db.conn.cursor()
        cur.execute("SELECT * INTO TEMPORARY delobj FROM activity_out_all WHERE aid = %s;", [activity.aid])
        cur.execute('DELETE FROM object WHERE oid IN (select oid from delobj);')
        cur.execute('DELETE FROM activation WHERE avid IN (select distinct avid from delobj);')

    def add_activity(self, module, args, kindtags_in, kindtags_out, stateless=True):
        add_missing_tags(kindtags_in)
        add_missing_tags(kindtags_out)
        cur = self._db.conn.cursor()
        cur.execute("INSERT INTO activity (createtime, jid, module, args, kindtags_out, stateless) VALUES (clock_timestamp(), %s, %s, %s, %s, %s) RETURNING aid;", [self.jid, module, args, json.dumps(kindtags_out), stateless])
        aid = singlevalue(cur)
        for trigger in kindtags_in:
            cur.execute("INSERT INTO activity_trigger (aid, kindtags) VALUES (%s, %s);", [aid, json.dumps(trigger)])
        self._db.add_log('activity.create', {'aid': aid, 'jid': self.jid, 'module': module, 'kindtags_in': kindtags_in, 'kindtags_out': kindtags_out})
        # do not forget to moify the cache
        return self._db.get_activity(aid)

    def add_seed_data(self, objects):
        seed = []
        for obj in objects:
            if obj.lightweight():
                newobj = self._db.create_object(self, None, obj.kindtags, obj.metadata, obj.str_data(), obj.bytes_data, obj.json_data, obj.sentence)
            else:
                newobj = obj
                print("add_seed_data: Unexpected Object: "+str(obj))
            seed.append(newobj.oid)
        cur = self._db.conn.cursor()
        cur.execute("UPDATE job SET seed=%s WHERE jid=%s ;", [seed, self.jid])


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
    pdb = None
    db_config = {}
    for key in ('database', 'user', 'host', 'password'):
        if config.has_option('postgres', key):
            db_config[key] = config.get('postgres', key)
    return PostgresConnection(db_config)
