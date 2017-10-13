import sys
import select
import time
import traceback
from threading import Thread
import configparser
import psycopg2
import psycopg2.extras

import storage.postgres

VERBOSE = False

verbose = print if VERBOSE else lambda *a, **k: None


class TaskQueue:

    def __init__(self, conn, id):
        self.conn = conn
        self.id = id
        self.listening = False
        verbose(self.id + ": init")

    def poll_task(self):
        verbose(self.id + ": poll_task")
        while True:
            if select.select([self.conn], [], [], 60) == ([], [], []):
                    verbose("***** SELECT Timeout")
            else:
                self.conn.poll()
                if self.conn.notifies:
                    self.conn.notifies.pop()
                    verbose(self.id + ": poll_task: succes")
                    return
                else:
                    verbose(self.id + ": poll_task: fail")

    def listen(self):
        cur = self.conn.cursor()
        cur.execute('LISTEN task_produced;')
        self.listening = True
        verbose(self.id + ": listen")

    def unlisten(self):
        cur = self.conn.cursor()
        cur.execute('UNLISTEN task_produced;')
        self.listening = False
        verbose(self.id + ": unlisten")

    def notify(self):
        cur = self.conn.cursor()
        cur.execute('NOTIFY task_produced;')
        verbose(self.id + ": notify")

    def create_tables(self):
        try:
            verbose(self.id + ": create tables")
            cur = self.conn.cursor()
            stat = """
                   CREATE TABLE task (id BIGSERIAL, jid BIGINT, aid BIGINT,
                                      oid BIGINT,
                                      slavetask BOOLEAN DEFAULT TRUE,
                                      version_id BIGINT);
                   CREATE TABLE jobstatus (jid BIGINT PRIMARY KEY,
                                           version_id BIGINT);
                   INSERT INTO jobstatus (jid,version_id) VALUES (0, 0);
               """
            cur.execute(stat)
        except Exception as ex:
            handle_exception(ex)

    def drop_tables(self):
        try:
            verbose(self.id + ": drop tables")
            cur = self.conn.cursor()
            stat = """
                  DROP TABLE IF EXISTS task CASCADE;
                  DROP TABLE IF EXISTS jobstatus CASCADE;
               """
            cur.execute(stat)
        except Exception as ex:
            handle_exception(ex)

    def start_job(self, job):
        jid = job.jid
        version_id = job.version_id
        try:
            verbose(self.id + ": add_job", jid)
            cur = self.conn.cursor()
            cur.execute("DELETE FROM jobstatus WHERE jid = %s;", [jid, ])
            cur.execute("INSERT INTO jobstatus(jid, version_id) \
                        VALUES(%s, %s);", [jid, version_id])
        except Exception as ex:
            handle_exception(ex)

    def suspend_job(self, job):
        try:
            jid = job.jid
            verbose(self.id + ": suspend_job", jid)
            new_version_id = job.new_version()
            cur = self.conn.cursor()
            cur.execute("DELETE FROM jobstatus WHERE jid = %s;", [jid, ])
            cur.execute("UPDATE task SET version_id=" + str(new_version_id) +
                        " WHERE jid="+str(jid)+";")
        except Exception as ex:
            handle_exception(ex)

    def push_task(self, jid, aid, oid, slavetask, version_id):
        try:
            verbose(self.id + ": push_task", jid, aid, oid, slavetask)
            cur = self.conn.cursor()
            cur.execute("INSERT INTO task (jid, aid, oid, slavetask,\
                        version_id) VALUES (%s, %s, %s, %s, %s);",
                        [jid, aid, oid, slavetask, version_id])
        except Exception as ex:
            handle_exception(ex)

    def notify_tasks(self):
        try:
            verbose(self.id + ": notify_tasks")
            self.notify()
        except Exception as ex:
            handle_exception(ex)

    def pop_task(self, slavetask=None):
        if slavetask is None:
            where = ""
        else:
            if slavetask:
                where = "WHERE slavetask = TRUE AND task.jid = jobstatus.jid"
            else:
                where = "WHERE slavetask = FALSE AND task.jid = jobstatus.jid"
        try:
            cur = self.conn.cursor()
            stat = """
                DELETE FROM task
                WHERE id = (
                  SELECT id
                  FROM task, jobstatus
                """ \
                + where + \
                """ \
                  ORDER BY id
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                RETURNING *;
               """
            cur.execute(stat)
            return singlerow(cur)
        except Exception as ex:
            handle_exception(ex)


def singlerow(cur):
    if cur.rowcount == 0:
        verbose("pop_task: "+"None")
        return None
    else:
        rows = cur.fetchall()
        verbose("pop_task: "+str(rows))
        return rows[0]


class Producer:

    def __init__(self, id):
        self.id = id
        verbose('Producer: ' + self.id + ": started")
        pdb = postgres.opendb('../master.local.cfg')
        taskq = TaskQueue(pdb.conn, id)
        taskq.drop_tables()
        taskq.create_tables()
        while True:
            taskq.push_task(99, 100, 101, True)
            taskq.push_task(99, 100, 101, True)
            taskq.notify_tasks()
            time.sleep(5)


def run_producer(id):
    prod = Producer(id)


class Consumer:

    def __init__(self, id):
        self.id = id
        pdb = postgres.opendb('../master.local.cfg')
        self.taskq = TaskQueue(pdb.conn, id)
        self.run_loop()

    def process_task(self, task):
        verbose(self.id + ": PROCESS TASK: " + str(task))
        time.sleep(1)

    def run_loop(self):
        while True:
            task = self.taskq.pop_task(True)
            if task is None:
                verbose(self.id + ": NO TASK")
                self.taskq.commit()  # necessary, consumer may block
                if not self.taskq.listening:
                    self.taskq.listen()
                self.taskq.poll_task()
            else:
                if self.taskq.listening:
                    self.taskq.unlisten()
                self.process_task(task)


def run_consumer(id):
    cons = Consumer(id)


def handle_exception(ex):
    print('EXCEPTION' + ': ' + str(ex),  file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit()

if __name__ == '__main__':
    Thread(name='prod', target=run_producer, args=('producer',)).start()
    time.sleep(2)
    Thread(name='cons-1', target=run_consumer, args=('consumer-1',)).start()
    Thread(name='cons-2', target=run_consumer, args=('consumer-2',)).start()
