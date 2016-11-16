import sys
import socket
import configparser
from collections import defaultdict
import engine
import storage


class Scheduler:

    # inspired by http://stacktory.com/blog/2015/why-not-postgres-1-multi-consumer-fifo-push-queue.html
    # HINT, there is also a push mode in this page, no active waits!!!
    def __init__(self, configfile, db):
        self.db = db
        #
        config = configparser.RawConfigParser()
        config.read(configfile)
        self.role = config.get("scheduler", "role")
        if self.role == "master":
            self.master = True
            self.prefer_master_task = (config.get("scheduler", "task") == "master")
        else:
            self.master = False
            self.prefer_master_task = False
        self.mode = config.get("scheduler", "mode")
        self.batchsize = int(config.get("scheduler", "batchsize"))
        self.host = socket.gethostbyaddr(socket.gethostname())[0]
        self.hostid = self.db.add_log('host.start', {'role': self.role, 'host': self.host}, flush=True)
        #
        if self.mode == "start":
            self.db.add_log('debug.sched.start', {'hostid': self.hostid})
            self.destroy()
            self.create()
        elif self.mode == "restart":
            self.db.add_log('debug.sched.restart', {'hostid': self.hostid})
            self.restart()
        else:
            # deamons just join
            self
        #
        self.reset()

    def commit(self):
        self.db.commit()

    def reset(self):
        self.db.add_log('debug.sched.reset', {'hostid': self.hostid})
        # create the two job/activity info tables
        self.aid_slave = {}
        self.job_matches = {}
        #
        self.active_jobs = [dbj for dbj in self.db.active_jobs()]
        for job in self.active_jobs:
            self.add_job(job)
        # INCOMPLETE, maybe this code in restart:
        # finally reset all assigned transactions
        cur = self.db.conn.cursor()
        cur.execute("UPDATE task SET assigned=NULL WHERE assigned IS NOT NULL;")

    def create(self):
        try:
            cur = self.db.conn.cursor()
            # Create table of pending tasks, assigned attr show if task is assigned
            # When task is finished state record is deleted
            stat = """
                CREATE TABLE task (id BIGSERIAL, jid BIGINT, aid BIGINT, oid BIGINT, assigned BIGINT DEFAULT NULL, slavetask BOOLEAN DEFAULT TRUE);
                CREATE OR replace FUNCTION public.all_pending_tasks (integer, integer, bigint) RETURNS SETOF task AS
                $$
                DECLARE
                    r task % rowtype;
                BEGIN
                    LOCK TABLE task IN EXCLUSIVE MODE;
                    FOR r IN
                        SELECT * FROM task
                        WHERE jid = $1 AND assigned IS NULL
                        ORDER BY id ASC
                        LIMIT $2
                    LOOP
                        UPDATE task SET assigned=$3 WHERE id=r.id RETURNING * INTO r;
                        RETURN NEXT r;
                  END LOOP;
                  RETURN;
                END
                $$ LANGUAGE plpgsql VOLATILE STRICT;
                CREATE OR replace FUNCTION public.pending_tasks (integer, boolean, integer, bigint) RETURNS SETOF task AS
                $$
                DECLARE
                    r task % rowtype;
                BEGIN
                    LOCK TABLE task IN EXCLUSIVE MODE;
                    FOR r IN
                        SELECT * FROM task
                        WHERE jid = $1 AND assigned IS NULL AND slavetask = $2
                        ORDER BY id ASC
                        LIMIT $3
                    LOOP
                        UPDATE task SET assigned=$4 WHERE id=r.id RETURNING * INTO r;
                        RETURN NEXT r;
                  END LOOP;
                  RETURN;
                END
                $$ LANGUAGE plpgsql VOLATILE STRICT;
            """
            cur.execute(stat)
        except Exception as ex:
            print(str(ex))
            storage.postgres.handle_db_error("create Scheduler", ex)

    def destroy(self):
        try:
            cur = self.db.conn.cursor()
            stat = """
                DROP TABLE IF EXISTS task CASCADE;
            """
            cur.execute(stat)
        except Exception as ex:
            storage.handle_db_error("destroy debug.sched", ex)
        self

    def restart(self):
        self

    def add_job(self, job):
        self.db.add_log('debug.sched.add_job', {'hostid': self.hostid, 'jid': job.jid})
        if job.jid in self.job_matches:
            raise Exception("duplicate job")
        match_kind_tags = self.job_matches[job.jid] = defaultdict(list)
        #
        for activity in job.activities():
            for trigger in activity.activity_triggers():
                kind = trigger[0]
                match_kind_tags[kind].append([activity.aid, trigger[1]])
        if not job.initialized:
            for activity in job.activities():
                activity.set_initialized()
                self.aid_slave[activity.aid] = activity.stateless
            for oid in job.seed:
                self.db.add_log('object.seed', {'hostid': self.hostid, 'jid': job.jid, 'oid': oid})
                self.schedule_object(self.db.get_object(oid))
                job.set_initialized()
        else:
            # the job is initialized, check for uninitialized activities
            for activity in job.activities():
                self.aid_slave[activity.aid] = activity.stateless
                if not activity.initialized:
                    self.db.add_log('scheduler.trigger_activity', {'hostid': self.hostid, 'aid': activity.aid, 'module': activity.module})
                    self.trigger_activity(activity)
                    activity.set_initialized()

    def get_matching_activities(self, jid, kind, tags):
        result = set()
        for aid_tags in (self.job_matches[jid])[kind]:
            if aid_tags[1].issubset(tags):
                result.add(aid_tags[0])
        return result

    def add_tasks(self, s_jidaidoid):
        cur = self.db.conn.cursor()
        for jidaidoid in s_jidaidoid:
            cur.execute("INSERT INTO task (jid, aid, oid, slavetask) VALUES (%s, %s, %s, %s);", [jidaidoid[0], jidaidoid[1], jidaidoid[2], self.aid_slave[jidaidoid[1]]])

    def finish_tasks(self, s_jidaidoid):
        cur = self.db.conn.cursor()
        for jidaidoid in s_jidaidoid:
            cur.execute("DELETE FROM task WHERE jid = %s AND aid = %s AND  oid = %s;", [jidaidoid[0], jidaidoid[1], jidaidoid[2]])

    def pending_tasks(self, jid, n=None):
        if n is None:
            n = self.batchsize
        cur = self.db.conn.cursor()
        if (not self.master) or self.prefer_master_task:
            cur.execute("SELECT jid, aid, oid FROM pending_tasks(%s, %s, %s, %s);", [jid, (not self.master), n, self.hostid])
            qresult = cur.fetchall()
            if self.master and len(qresult) == 0 and self.prefer_master_task:
                cur.execute("SELECT jid, aid, oid FROM all_pending_tasks(%s, %s, %s);", [jid, n, self.hostid])
                qresult = cur.fetchall()
        else:
            cur.execute("SELECT jid, aid, oid FROM all_pending_tasks(%s, %s, %s);", [jid, n, self.hostid])
            qresult = cur.fetchall()
        res = [[row[0], row[1], row[2]] for row in qresult]
        return res

    def create_object(self, job, activation, obj):
        if obj.lightweight():
            newobj = self.db.create_object(job, activation, obj.kindtags(), obj.metadata(), obj.str_data(), obj.bytes_data(), obj.json_data())
        else:
            raise Exception("unexpected persisten object")
        self.db.add_log('object.create', {'hostid': self.hostid, 'oid': newobj.oid, 'kindtags': newobj.kindtags, 'jid': newobj.jid, 'avid':activation.avid}, flush=True)
        self.schedule_object(newobj)
        return newobj

    def schedule_object(self, obj):
        tasks = []
        for aid in self.get_matching_activities(obj.jid, obj.kindtags['kind'], set(obj.kindtags['tags'])):
            newtask = [obj.jid, aid, obj.oid]
            tasks.append(newtask)
        self.add_tasks(tasks)

    def print_tasks(self, header='TASKS'):
        print(header+':')
        cur = self.db.conn.cursor()
        cur.execute('SELECT jid, aid, oid FROM task;')
        rows = cur.fetchall()
        for row in rows:
            print(row[0], row[1], row[2])

    def trigger_activity(self, activity):
        tasks = []
        aid = activity.aid
        jid = activity.jid
        for oid in activity.oids_triggered():
            newtask = [jid, aid, oid]
            tasks.append(newtask)
        self.add_tasks(tasks)

    def reset_activity(self, activity):
        cur = self.db.conn.cursor()
        # (1) compute recursively all activations generated by this activity
        # recursively follwing all oid-in-out links and all rid-in-out links
        cur.execute("SELECT * INTO TEMPORARY sptree FROM all_avid_descendants WHERE aid = %s;", [activity.aid])
        #
        # (2) compute all input objects of these activations
        cur.execute("SELECT DISTINCT activation.aid, oid INTO TEMPORARY sptree_in FROM sptree, activation, unnest(activation.oid_in) AS oid WHERE sptree.avid=activation.avid;")
        #
        # (3) compute all objects generated by these activations
        cur.execute("SELECT DISTINCT activation.aid, oid INTO TEMPORARY sptree_out FROM sptree, activation, unnest(activation.oid_out) AS oid WHERE sptree.avid=activation.avid;")
        #
        # (4) compute all object which have to be retriggered bu substracting
        # the out objects (3) from the in objects
        cur.execute("SELECT sptree_in.aid, sptree_in.oid FROM sptree_in LEFT JOIN sptree_out ON sptree_in.oid = sptree_out.oid WHERE sptree_out.oid IS NULL;")
        #
        # INCOMPLETE, remove all out resources
        tasks = []
        rows = cur.fetchall()
        jid = activity.jid
        for row in rows:
            newtask = [jid, row[0], row[1]]
            tasks.append(newtask)
        self.add_tasks(tasks)
        #
        # delete all out objects (3)
        cur.execute('DELETE FROM object WHERE oid IN (select oid from sptree_out);')
        #
        # delete all out activations computed in (3)
        cur.execute('DELETE FROM activation WHERE avid IN (select distinct avid from sptree);')
