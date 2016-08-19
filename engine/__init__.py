import sys
from threading import Thread
import traceback
import logging
import configparser
import storage
from engine.scheduler import Scheduler


class LwObject:

    def __init__(self, a_kindtags, a_metadata, a_raw_data, a_json_data="{}"):
        self.a_kindtags = a_kindtags
        self.a_metadata = a_metadata
        self.a_raw_data = a_raw_data
        self.a_json_data = a_json_data
        self._delayed_oid_container = [-1]

    def lightweight(self):
        return True

    def kindtags(self):
        return self.a_kindtags

    def metadata(self):
        return self.a_metadata

    def raw_data(self):
        return self.a_raw_data

    def json_data(self):
        return self.a_json_data

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
        self.kindtags_out = self.db_activity.kindtags_out()
        self.kindtags_default = self.kindtags_out[0]
        self.module = self.db_activity.module()
        self.scheduler = context['scheduler']
        self.setup([arg.strip() for arg in context['args'].split(',')])
        logging.info("Activity:"+self.db_activity.module() + " start")

    def setup(self, args):
        logging.info("Activity:"+self.db_activity.module() + " setup() ignored")

    def allow_distribution(self):
        self.scheduler.allow_distribution(self.db_activity.aid())

    def triggers(self):
        return self.db_activity.trigger

    def objects_in(self):
        return self.db_activity.objects_in()

    def objects_out(self):
        return self.db_activity.objects_out()

    def get_object(self, oid):
        return self.db.get_object(oid)

    def new_activation(self, inobj, outobj):
        activation = self.db.add_activation(self.db_activity.aid())
        persistent_out = []
        for obj in outobj:
            if obj.lightweight():
                newobj = self.scheduler.create_object(self.job, activation, obj)
                obj.set_delayed_oid(newobj.oid())
                persistent_out.append(newobj)
            else:
                persistent_out.append(mix)
        self.db.set_activation_graph(activation, inobj, persistent_out)
        activation.set_status('f')
        return activation

    def handle_simple(self, obj):
        raise Exception("handle_simple() missing")

    def handle_complex(self, obj):
        outobj = self.handle_simple(obj)
        activation = self.new_activation([obj, ], outobj)
        self.db.add_log("activation.finish", {'module': self.module, 'id': self.scheduler.id, 'oid': obj.oid(), 'avid': activation.avid()})

    def process_object(self, o):
        logging.info(self.db_activity.module()+": handle_object(aid="+str(self.db_activity.aid())+", oid="+str(o.oid())+") start")
        try:
            self.handle_complex(o)
        except Exception as ex:
            error_str = "EXCEPTION in module " + self.db_activity.module()\
                         + " on oid[" + str(o.oid()) + "]: " + str(ex)\
                         + '\n' + traceback.format_exc()
            # self.handler.activation.set_status('e')
            # self.db.add_log(self.db_activity.aid(), "activation.error", error_str)
            self.db.add_log("activation.error", {'module': self.module, 'id': self.scheduler.id, 'error': error_str})
            logging.info(self.db_activity.module()+": handle_object(aid="+str(self.db_activity.aid())+", oid="+str(o.oid())+") error")
            logging.error(self.db_activity.module()+":"+error_str)
            if (True):
                # TODO do not stop here, be sensible
                print(error_str, file=sys.stderr)
                self.db.closedb()
                sys.exit()
            return
        #
        logging.info(__name__+": handle_object(aid="+str(self.db_activity.aid())+", oid="+str(o.oid())+") finish")


def create_activity(db, scheduler, job, db_activity):
    module = __import__(db_activity.module(), fromlist=[''])
    activity = module.get_handler({'db': db, 'scheduler': scheduler, 'job': job, 'db_activity': db_activity, 'args': db_activity.args()})
    return activity

def run_job(configfile, scheduler, job):
    db = storage.opendb(configfile)
    active = {}
    while True:
        # get the pending jobs, scheduler say how much you will get
        todo = scheduler.pending_tasks(job.jid())
        if len(todo) == 0:
            logging.info("run_job: no more todo")
            break
        else:
            logging.info("run_job: get todo: len="+str(len(todo)))
        for task in todo:
            aid = task[1]
            activity = active.get(aid)
            if activity is None:
                activity = create_activity(db, scheduler, job, db.get_activity(aid))
                active[aid] = activity
            activity.process_object(db.get_object(task[2]))
        scheduler.rm_tasks(todo)
    db.closedb()


def start(configfile):
    """
    Run the Combine engine
    """
    logging.info("Running the Combine Web Harvesting engine!")
    #
    db = storage.opendb(configfile)
    scheduler = Scheduler(configfile)
    joblist = []
    for job in db.active_jobs():
        jobthread = Thread(name="Job:"+job.name(), target=run_job, args=(configfile, scheduler, job, ))
        scheduler.add_job(job)
        joblist.append((job, jobthread))
        jobthread.start()
    # db.closedb() error
