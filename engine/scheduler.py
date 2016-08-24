import sys
import logging
import socket
import configparser
from collections import defaultdict
import engine
import storage


class Scheduler:

    # inspired by http://stacktory.com/blog/2015/why-not-postgres-1-multi-consumer-fifo-push-queue.html
    # HINT, there is also a push mode in this page, no active waits!!!
    def __init__(self, configfile, db):
        logging.info("scheduler: started")
        #
        config = configparser.RawConfigParser()
        config.read(configfile)
        self.id = config.get("scheduler", "id")
        self.mode = config.get("scheduler", "mode")
        self.batchsize = int(config.get("scheduler", "batchsize"))
        #
        if self.id == "slave":
            self.id = socket.gethostbyaddr(socket.gethostname())[0]
        #
        # self.db = storage.opendb(configfile)
        self.db = db
        if self.mode == "start":
            self.destroy()
            self.create()
        elif self.mode == "restart":
            self.restart()
        else:
            # deamons just join
            self
        # self.match_kind_tags = defaultdict(list)
        self.job_matches = {}
        #
        # self.add_tasks([[7,11,22],[7,33,44],[7,66,77],[7,88,99]])
        # self.rm_tasks([[7,11,22],[7,66,77],[7,88,99]])
        # print("######",str(self.pending_tasks(7,9)))

    def create(self):
        logging.info("scheduler: create()")
        try:
            cur = self.db.conn.cursor()
            # Create table of pending tasks, assigned attr show if task is assigned
            # When task is finished state record is deleted
            stat = """
                CREATE TABLE task (id BIGSERIAL, jid BIGINT, aid BIGINT, oid BIGINT, assigned BOOLEAN DEFAULT FALSE, stateless BOOLEAN DEFAULT TRUE);
                CREATE OR replace FUNCTION public.pending_tasks (integer, integer) RETURNS SETOF task AS
                $$
                DECLARE
                    r task % rowtype;
                BEGIN
                    LOCK TABLE task IN EXCLUSIVE MODE;
                    FOR r IN
                        SELECT * FROM task
                        WHERE jid = $1 AND assigned = FALSE
                        ORDER BY id ASC
                        LIMIT $2
                    LOOP
                        UPDATE task SET assigned=TRUE WHERE id=r.id RETURNING * INTO r;
                        RETURN NEXT r;
                  END LOOP;
                  RETURN;
                END
                $$ LANGUAGE plpgsql VOLATILE STRICT;
            """
            cur.execute(stat)
            self.db.conn.commit()
        except Exception as ex:
            storage.handle_db_error("create Scheduler", ex)

    def destroy(self):
        logging.info("scheduler: destroy()")
        try:
            cur = self.db.conn.cursor()
            stat = """
                DROP TABLE IF EXISTS task CASCADE;
            """
            cur.execute(stat)
            self.db.conn.commit()
        except Exception as ex:
            storage.handle_db_error("destroy scheduler", ex)
        self

    def restart(self):
        print("TODO: SCHEDULER RESTART NOT IMPLEMENTED YET")

    def add_job(self, job):
        logging.info("scheduler: add_job(jid="+str(job.jid)+")")
        if job.jid in self.job_matches:
            raise Exception("duplicate job")
        match_kind_tags = self.job_matches[job.jid] = defaultdict(list)
        #
        for activity in job.activities():
            for trigger in activity.activity_triggers():
                kind = trigger[0]
                match_kind_tags[kind].append([activity.aid, trigger[1]])
        if not job.initialized:
            for oid in job.seed:
                self.schedule_object(self.db.get_object(oid), False)
                job.set_initialized()
        self.db.conn.commit()

    def get_matching_activities(self, jid, kind, tags):
        result = set()
        for aid_tags in (self.job_matches[jid])[kind]:
            if aid_tags[1].issubset(tags):
                result.add(aid_tags[0])
        return result

    def rm_job(self, job):
        self

    def add_tasks(self, s_jidaidoid, commit=True):
        cur = self.db.conn.cursor()
        for jidaidoid in s_jidaidoid:
            cur.execute("INSERT INTO task (jid, aid, oid) VALUES (%s, %s, %s);", [jidaidoid[0], jidaidoid[1], jidaidoid[2]])
        if commit:
            self.db.conn.commit()

    def rm_tasks(self, s_jidaidoid):
        cur = self.db.conn.cursor()
        for jidaidoid in s_jidaidoid:
            cur.execute("DELETE FROM task WHERE jid = %s AND aid = %s AND  oid = %s;", [jidaidoid[0], jidaidoid[1], jidaidoid[2]])
        cur = self.db.conn.commit()

    def pending_tasks(self, jid, n=None, commit=True):
        if n is None:
            n = self.batchsize
        cur = self.db.conn.cursor()
        # TODO master should first get all master jobs and after that the "*"
        cur.execute("SELECT jid, aid, oid FROM pending_tasks(%s, %s);", [jid, n])
        res = [[row[0], row[1], row[2]] for row in cur.fetchall()]
        if commit:
            self.db.conn.commit()
        return res

    def create_object(self, job, activation, obj, commit=True):
        if obj.lightweight():
            newobj = self.db.create_object(job, activation, obj.kindtags(), obj.metadata(), obj.str_data(), obj.bytes_data(), obj.json_data(), commit=False)
        else:
            raise Exception("unexpected persisten object")
        self.schedule_object(newobj)
        return newobj

    def schedule_object(self, obj, commit=True):
        logging.info("scheduler: schedule_object(oid="+str(obj.oid)+")")
        tasks = []
        for aid in self.get_matching_activities(obj.jid, obj.kindtags['kind'], set(obj.kindtags['tags'])):
            newtask = [obj.jid, aid, obj.oid]
            tasks.append(newtask)
            logging.info("scheduler: new task "+str(newtask))
        self.add_tasks(tasks, commit)

    def print_tasks(self, header='TASKS'):
        print(header+':')
        cur = self.db.conn.cursor()
        cur.execute('SELECT jid, aid, oid FROM task;')
        rows = cur.fetchall()
        for row in rows:
            print(row[0], row[1], row[2])

    def reset_activity(self, activity, commit=True):
        oid_triggered = activity.oids_triggered(False)
        cur = self.db.conn.cursor()
        cur.execute("SELECT * INTO TEMPORARY delobj FROM activity_out_all WHERE aid = %s;", [activity.aid])
        cur.execute('DELETE FROM object WHERE oid IN (select oid from delobj);')
        cur.execute('DELETE FROM activation WHERE avid IN (select distinct avid from delobj);')
        #
        aid = activity.aid
        jid = activity.jid
        tasks = []
        for oid in oid_triggered:
            newtask = [jid, aid, oid]
            tasks.append(newtask)
            logging.info("scheduler: new task "+str(newtask))
        self.add_tasks(tasks, False)
        if commit:
            self.db.conn.commit()

