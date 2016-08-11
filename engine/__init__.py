import sys
from threading import Thread
import traceback
import logging
import configparser
import storage


class LwObject:

    def __init__(self, a_kind, a_tags, a_content_type, a_text, a_data):
        self.a_kind = a_kind
        self.a_tags = a_tags
        self.a_content_type = a_content_type
        self.a_text = a_text
        self.a_data = a_data

    def kind(self):
        return self.a_kind

    def tags(self):
        return self.a_tags

    def content_type(self):
        return self.a_content_type

    def text(self):
        return self.a_text

    def data(self):
        return self.a_data


class Activation:

    def __init__(self, context):
        self.context = context
        # tmp
        self.db = context['db']
        self.db_activity = context['db_activity']
        self.job = None
        self.activation = None

    def avid(self):
        return self.activation.avid()

    def start_activation(self):
        if not(self.activation is None):
            raise Exception("Start already started activation")
        self.activation = self.db.add_activation(self.db_activity.aid())
        if self.job is None:
            self.job = self.db.get_job(self.db_activity.jid())
        self.inobj = []
        self.lwoutobj = []
        self.shared_avid = None
        self.db.add_log(self.activation.avid(), "activation.start", "")

    def finish_activation(self):
        if self.shared_avid is None:
            outobj = []
            for o in self.lwoutobj:
                outobj.append(self.create_object(o))
        else:
            outobj = None
        self.db.set_activation_graph(self.activation, self.inobj, outobj, self.shared_avid)
        self.activation.set_status('f')
        self.db.add_log(self.activation.avid(), "activation.finish", "")
        self.reset_activation()

    def share_activation(self, avid):
        self.shared_avid = avid

    def reset_activation(self):
        self.activation = None

    def input(self, o):
        self.inobj.append(o)

    def output(self, o):
        self.lwoutobj.append(o)

    def create_object(self, lwo):
        newobj = self.db.add_object(self.job, self.activation, lwo.kind(), lwo.tags(), lwo.content_type(), lwo.text(), lwo.data())
        return newobj


class Activity:

    def __init__(self, context):
        self.context = context
        self.db = context['db']
        self.job = context['job']
        self.db_activity = context['db_activity']
        self.setup([arg.strip() for arg in context['args'].split(',')])
        # TODO store triggers in Python obj
        logging.info("Activity:"+self.db_activity.module() + " start")

    def setup(self, args):
        logging.info("Activity:"+self.db_activity.module() + " setup() ignored")

    def triggers(self):
        return self.db_activity.trigger

    def objects_out(self):
        return self.db_activity.objects_out()

    def process_object(self, o):
        logging.info(self.db_activity.module()+": handle_object(aid="+str(self.db_activity.aid())+", oid="+str(o.oid())+") start")
        #
        self.activation = Activation(self.context)
        self.activation.start_activation()
        try:
            self.handle(self.activation, o)
        except Exception as ex:
            error_str = "EXCEPTION in module " + self.db_activity.module()\
                         + " on oid[" + str(o.oid()) + "]: " + str(ex)\
                         + '\n' + traceback.format_exc()
            self.activation.activation.set_status('e')
            self.db.add_log(self.activation.activation.avid(), "activation.error", error_str)
            logging.info(self.db_activity.module()+": handle_object(aid="+str(self.db_activity.aid())+", oid="+str(o.oid())+") error")
            logging.error(self.db_activity.module()+":"+error_str)
            self.activation.reset_activation()
            if (True):
                # TODO do not stop here, be sensible
                print(error_str, file=sys.stderr)
                sys.exit()
            return
        self.activation.finish_activation()
        #
        logging.info(__name__+": handle_object(aid="+str(self.db_activity.aid())+", oid="+str(o.oid())+") finish")


def create_activity(db, job, db_activity):
    module = __import__(db_activity.module(), fromlist=[''])
    activity = module.get_handler({'db': db, 'job': job, 'db_activity': db_activity, 'args': db_activity.args()})
    return activity


def run_job(configfile, job):
    db = storage.opendb(configfile)
    active = {}
    while True:
        todo = db.objects_todo(job)
        if len(todo) == 0:
            logging.info("run_job: no more todo")
            break
        else:
            logging.info("run_job: get todo: len="+str(len(todo)))
        for ao in todo:
            activity = active.get(ao[0])
            if activity is None:
                activity = create_activity(db, job, db.get_activity(ao[0]))
                active[ao[0]] = activity
            activity.process_object(db.get_object(ao[1]))
    db.closedb()


def start(configfile):
    """
    Run the Combine engine
    """
    logging.info("Running the Combine Web Harvesting engine!")
    db = storage.opendb(configfile)
    joblist = []
    for job in db.active_jobs():
        jobthread = Thread(name="Job:"+job.name(), target=run_job, args=(configfile, job, ))
        joblist.append((job, jobthread))
        jobthread.start()
    # db.closedb() error
