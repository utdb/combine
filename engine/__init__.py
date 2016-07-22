from threading import Thread
import logging
import configparser

import storage

class basic_handler:

    def __init__(self,context):
        self.context = context

    def add2in(self,o):
        self.context['activity'].add2in(o)

    def add2out(self,o):
        self.context['activity'].add2out(o)

    def create_object(self,kind,tags,content_type,content):
        return self.context['activity'].create_object(kind,tags,content_type,content)

class activity_handler:

    def __init__(self,db,job,activity):
        self.db = db
        self.job = job
        self.activity = activity
        self.module =  __import__(self.activity.module(), fromlist=[''])
        self.handler = self.module.get_handler({'db':self.db,'job':job,'activity':self.activity,'args':activity.args()})
        logging.info("activity_handler:"+self.activity.module() + " start")

    def handle_object(self,o):
        logging.info(__name__+": handle_object(aid="+str(self.activity.aid())+",oid="+str(o.oid())+") start")
        #
        self.activity.start_activation()
        self.handler.handle_object(o)
        self.activity.finish_activation()
        #
        logging.info(__name__+": handle_object(aid="+str(self.activity.aid())+",oid="+str(o.oid())+") finish")

def run_job(configfile,job):
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
            a = active.get(ao[0])
            if a is None:
                a = activity_handler(db,job,db.get_activity(ao[0]))
                active[ao[0]] = a
            a.handle_object(db.get_object(ao[1]))
    db.closedb()

def start(configfile):
    """
    Run the Combine engine
    """
    logging.info("Running the Combine Web Harvesting engine!")
    db = storage.opendb(configfile)
    joblist = []
    for job in db.active_jobs():
        jobthread = Thread(name="Job:"+job.name(),target=run_job,args=(configfile,job,))
        joblist.append((job,jobthread))
        jobthread.start()
    # db.closedb() error
