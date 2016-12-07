import sys
from threading import Thread
import traceback
import configparser
import storage
from engine.scheduler import Scheduler
import psycopg2


class LwObject:

    def __init__(self, a_kindtags, a_metadata, a_str_data, a_bytes_data, a_json_data, a_sentence):
        self.oid = -1
        self.kindtags = a_kindtags
        self.metadata = a_metadata
        self.a_str_data = a_str_data
        self.bytes_data = a_bytes_data
        self.json_data = a_json_data
        self.sentence = a_sentence
        self._delayed_oid_container = [-1]

    def lightweight(self):
        return True

    def str_data(self):
        return self.a_str_data

    def delayed_oid_container(self):
        return self._delayed_oid_container

    def set_delayed_oid(self, v):
        self._delayed_oid_container[0] = v


class Activity:

    def __init__(self, context):
        self.context = context
        self.db = context['db']
        self.job = context['job']
        self.db_activity = context['db_activity']
        self.kindtags_out = self.db_activity.kindtags_out
        self.kindtags_default = self.kindtags_out[0]
        self.module = self.db_activity.module
        self.scheduler = context['scheduler']
        self.setup([arg.strip() for arg in context['args'].split(',')])

    def setup(self, args):
        self

    def allow_distribution(self):
        self.scheduler.allow_distribution(self.db_activity.aid)

    def triggers(self):
        return self.db_activity.trigger

    def objects_in(self):
        return self.db_activity.objects_in()

    def objects_out(self):
        return self.db_activity.objects_out()

    def get_object(self, oid):
        return self.db.get_object(oid)

    def new_activation(self, inobj, outobj, inrsrc=None, outrsrc=None):
        activation = self.db.add_activation(self.db_activity.aid)
        persistent_out = []
        inobj_s = [obj.oid for obj in inobj]
        for obj in outobj:
            if obj.lightweight():
                newobj = self.scheduler.create_object(self.job, activation, obj)
                obj.set_delayed_oid(newobj.oid)
                persistent_out.append(newobj)
            else:
                persistent_out.append(mix)
        self.db.set_activation_graph(activation, inobj, persistent_out, inrsrc, outrsrc)
        activation.set_status('f')
        self.db.add_log('activation.create', {'hostid': self.scheduler.hostid, 'jid': self.job.jid, 'avid': activation.avid, 'inobj': inobj_s})
        return activation

    def activity_label(self):
        dba = self.db_activity
        return dba.module + '_' + str(dba.aid)

    def get_resource(self, label, create):
        return self.db.get_resource(label, create)

    def handle_simple(self, obj):
        raise Exception("handle_simple() missing")

    def handle_complex(self, obj):
        outobj = self.handle_simple(obj)
        activation = self.new_activation([obj, ], outobj)

    def process_object(self, db, o):
        try:
            self.handle_complex(o)
        except Exception as ex:
            error_txt = "EXCEPTION in module " + self.db_activity.module\
                         + " on oid[" + str(o.oid) + "]: " + str(ex)\
                         + '\n' + traceback.format_exc()
            db.last_error = {'module': self.module, 'id': self.scheduler.role, 'error': error_txt}
            raise


def create_activity(db, scheduler, job, db_activity):
    module = __import__(db_activity.module, fromlist=[''])
    activity = module.get_handler({'db': db, 'scheduler': scheduler, 'job': job, 'db_activity': db_activity, 'args': db_activity.args})
    return activity


def run_job_OLD(configfile, scheduler, job, db):
    active = {}
    while True:
        # get the pending jobs, scheduler say how much you will get
        todo = scheduler.pending_tasks(job.jid)
        if todo is None:
            scheduler.commit() # necessary, otherwise consumer may block
            if not scheduler.tq.listening:
                scheduler.tq.listen()
            scheduler.commit()
            scheduler.tq.poll_task()
        else:
            if scheduler.tq.listening:
                scheduler.tq.unlisten()
            process_task(todo, active, scheduler, job, db)
            scheduler.tq.notify_tasks()
            scheduler.commit()

def run_job(configfile, scheduler, job, db):
    active = {}
    MAXIMUM_BACKOFF = 12 * 60 * 60
    INITIAL_BACKOFF = 30 
    backoff_time = INITIAL_BACKOFF
    while True:
        try:
            if db.conn is None:
                db.reconnect()
            with db.conn:
                # get the pending jobs, scheduler say how much you will get
                todo = scheduler.pending_tasks(job.jid)
                if todo is None:
                    scheduler.commit() # necessary, otherwise consumer may block
                    if not scheduler.tq.listening:
                        scheduler.tq.listen()
                    scheduler.commit()
                    scheduler.tq.poll_task()
                else:
                    if scheduler.tq.listening:
                        scheduler.tq.unlisten()
                    process_task(todo, active, scheduler, job, db)
                    scheduler.tq.notify_tasks()
                    scheduler.commit()
        except psycopg2.Error as pe:
            db.rollback()
            db.force_log_message()
            print('Postgres Error:\n'+str(pe),file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            # try to reconnect to the sertver
            if backoff_time < MAXIMUM_BACKOFF:
                time.sleep(backoff_time)
                backoff_time = backoff_time*2
                # remove current connection and start with a fresh one
                db.closedb()
            else:
                break
        except (KeyboardInterrupt, SystemExit): # ?? BaseException
            db.rollback()
            raise
        except Exception as general_e:
            db.rollback()
            db.force_log_message()
            print('Exception:\n'+str(general_e),file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            sys.exit()



def process_task(task, active, scheduler, job, db):
    aid = task[2]
    activity = active.get(aid)
    if activity is None:
        activity = create_activity(db, scheduler, job, db.get_activity(aid))
        active[aid] = activity
    activity.process_object(db, db.get_object(task[3]))


class Engine:

    def __init__(self, configfile):
        """
        Run the Combine engine
        """
        self.configfile = configfile
        self.db = storage.opendb(configfile)
        self.scheduler = Scheduler(configfile, self.db)
        self.scheduler.commit()

    def run(self):
        self.joblist = []
        # always reset the scheduler before a run
        self.scheduler.reset()
        for job in self.scheduler.active_jobs:
            run_job(self.configfile, self.scheduler, job, self.db)
            # TODO: reimplement threading
            # jobthread = Thread(name="Job:"+job.name(), target=run_job, args=(self.configfile, self.scheduler, job, self.db ))
            # self.joblist.append((job, jobthread))
            # jobthread.start()

    def stop(self):
        self.db.closedb()
